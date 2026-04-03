#!/usr/bin/env python3
"""
Build a fresh PR-local map of changed source files to daily test files.

The mapper is intentionally conservative:
- broad-risk changes fall back to the full daily workflow
- direct test-file changes are always included
- source-file changes use Clang source-based coverage to find a small set of
  covering tests in each runner family
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
TMP_ROOT = ROOT / "tests" / "tmp" / "impact-map"
SOURCE_SUFFIXES = {".c", ".cc", ".cpp"}
EXCLUDED_MAIN_TESTS = {
    "integration/valkey-benchmark",
}
BROAD_RISK_PREFIXES = (
    ".github/",
    "deps/",
    "runtest",
    "tests/helpers/",
    "tests/support/",
    "tests/assets/",
    "tests/instances.tcl",
    "tests/test_helper.tcl",
    "tests/cluster/tests/includes/",
    "tests/sentinel/tests/includes/",
    "Makefile",
    "src/Makefile",
)


class CommandError(RuntimeError):
    pass


def run(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if proc.returncode != 0:
        tail = ""
        if capture_output:
            output = (proc.stdout or "") + "\n" + (proc.stderr or "")
            tail = "\n" + "\n".join(output.strip().splitlines()[-40:])
        raise CommandError(f"command failed ({proc.returncode}): {' '.join(cmd)}{tail}")
    return proc


def normalize(path: str | Path) -> str:
    return Path(path).as_posix().lstrip("./")


def git_changed_files(base_sha: str, head_sha: str) -> list[str]:
    proc = run(
        [
            "git",
            "diff",
            "--name-only",
            "--diff-filter=AMRT",
            f"{base_sha}...{head_sha}",
        ],
        capture_output=True,
    )
    return sorted({line.strip() for line in proc.stdout.splitlines() if line.strip()})


def is_broad_risk(path: str) -> bool:
    if any(path == prefix or path.startswith(prefix) for prefix in BROAD_RISK_PREFIXES):
        return True
    suffix = Path(path).suffix
    if suffix not in {".h", ".hh", ".hpp"}:
        return False

    # Repository-local source headers often change alongside the implementation
    # files we can map with fresh coverage, so they should not force a full
    # daily fallback on their own.
    if path.startswith("src/") or path.startswith("modules/lua/"):
        return False

    return True


def is_eligible_source(path: str) -> bool:
    suffix = Path(path).suffix
    if suffix not in SOURCE_SUFFIXES:
        return False
    return path.startswith("src/") or path.startswith("modules/lua/")


def canonical_main_test(path: Path) -> str | None:
    tests_root = ROOT / "tests"
    allowed_dirs = (
        tests_root / "unit",
        tests_root / "unit" / "type",
        tests_root / "unit" / "cluster",
        tests_root / "integration",
    )
    for base in allowed_dirs:
        try:
            rel = path.relative_to(base)
        except ValueError:
            continue
        return normalize(base.relative_to(tests_root) / rel.with_suffix(""))
    return None


def canonical_moduleapi_test(path: Path) -> str | None:
    base = ROOT / "tests" / "unit" / "moduleapi"
    try:
        rel = path.relative_to(base)
    except ValueError:
        return None
    return normalize(Path("unit/moduleapi") / rel.with_suffix(""))


def canonical_cluster_test(path: Path) -> str | None:
    base = ROOT / "tests" / "cluster" / "tests"
    try:
        rel = path.relative_to(base)
    except ValueError:
        return None
    return normalize(Path("tests") / rel)


def canonical_sentinel_test(path: Path) -> str | None:
    base = ROOT / "tests" / "sentinel" / "tests"
    try:
        rel = path.relative_to(base)
    except ValueError:
        return None
    return normalize(Path("tests") / rel)


def list_tests(base: Path, mapper) -> list[str]:
    return sorted(
        test
        for path in sorted(base.glob("*.tcl"))
        if (test := mapper(path)) is not None
    )


def join_single_args(tests: Iterable[str]) -> str:
    tests = sorted(dict.fromkeys(tests))
    return " ".join(f"--single {shlex.quote(test)}" for test in tests)


def find_tool(candidates: list[str]) -> str:
    for name in candidates:
        path = shutil.which(name)
        if path:
            return path
    raise CommandError(f"required tool not found: {' or '.join(candidates)}")


class ImpactMapper:
    def __init__(
        self,
        changed_files: list[str],
        *,
        max_tests_per_file: int,
        max_selected_tests: int,
        skip_build: bool,
    ) -> None:
        self.changed_files = changed_files
        self.max_tests_per_file = max_tests_per_file
        self.max_selected_tests = max_selected_tests
        self.skip_build = skip_build
        self.direct_main: set[str] = set()
        self.direct_moduleapi: set[str] = set()
        self.direct_cluster: set[str] = set()
        self.direct_sentinel: set[str] = set()
        self.eligible_sources = sorted(path for path in changed_files if is_eligible_source(path))
        self.group_cache: dict[tuple[str, tuple[str, ...]], set[str]] = {}
        self.llvm_cov = ""
        self.llvm_profdata = ""
        self.binaries: list[str] = []
        self.main_tests = list_tests(ROOT / "tests" / "unit", canonical_main_test)
        self.main_tests += list_tests(ROOT / "tests" / "unit" / "type", canonical_main_test)
        self.main_tests += list_tests(ROOT / "tests" / "unit" / "cluster", canonical_main_test)
        self.main_tests += list_tests(ROOT / "tests" / "integration", canonical_main_test)
        self.main_tests = sorted(
            test for test in dict.fromkeys(self.main_tests) if test not in EXCLUDED_MAIN_TESTS
        )
        self.moduleapi_tests = list_tests(ROOT / "tests" / "unit" / "moduleapi", canonical_moduleapi_test)
        self.cluster_tests = list_tests(ROOT / "tests" / "cluster" / "tests", canonical_cluster_test)
        self.sentinel_tests = list_tests(ROOT / "tests" / "sentinel" / "tests", canonical_sentinel_test)

    def build_response(self, *, mode: str, reason: str, uncovered: Iterable[str] = ()) -> dict:
        reason = " ".join(reason.split())
        main_args = join_single_args(self.direct_main)
        moduleapi_args = join_single_args(self.direct_moduleapi)
        cluster_args = join_single_args(sorted(self.direct_cluster | self.direct_sentinel))
        return {
            "mode": mode,
            "reason": reason,
            "changed_files": self.changed_files,
            "eligible_sources": self.eligible_sources,
            "uncovered_sources": sorted(uncovered),
            "main_tests": sorted(self.direct_main),
            "moduleapi_tests": sorted(self.direct_moduleapi),
            "cluster_tests": sorted(self.direct_cluster),
            "sentinel_tests": sorted(self.direct_sentinel),
            "main_test_args": main_args,
            "moduleapi_test_args": moduleapi_args,
            "cluster_test_args": cluster_args,
        }

    def collect_direct_tests(self) -> str | None:
        for changed in self.changed_files:
            path = ROOT / changed
            main = canonical_main_test(path)
            if main:
                self.direct_main.add(main)
                continue

            moduleapi = canonical_moduleapi_test(path)
            if moduleapi:
                self.direct_moduleapi.add(moduleapi)
                continue

            cluster = canonical_cluster_test(path)
            if cluster:
                self.direct_cluster.add(cluster)
                continue

            sentinel = canonical_sentinel_test(path)
            if sentinel:
                self.direct_sentinel.add(sentinel)
                continue

            if changed.startswith("tests/modules/") and path.suffix == ".c":
                moduleapi_path = ROOT / "tests" / "unit" / "moduleapi" / f"{path.stem}.tcl"
                if moduleapi_path.exists():
                    self.direct_moduleapi.add(canonical_moduleapi_test(moduleapi_path) or "")
                else:
                    return f"changed module source has no matching moduleapi test: {changed}"
        self.direct_moduleapi.discard("")
        return None

    def ensure_tooling(self) -> None:
        self.llvm_cov = find_tool(["llvm-cov", "llvm-cov-18", "llvm-cov-17", "llvm-cov-16"])
        self.llvm_profdata = find_tool(
            ["llvm-profdata", "llvm-profdata-18", "llvm-profdata-17", "llvm-profdata-16"]
        )

    def build_instrumented_binaries(self) -> None:
        if self.skip_build or not self.eligible_sources:
            return
        self.ensure_tooling()
        env = os.environ.copy()
        env["CC"] = "clang"
        env["OPTIMIZATION"] = "-O0"
        env["SERVER_CFLAGS"] = "-fprofile-instr-generate -fcoverage-mapping"
        env["SERVER_LDFLAGS"] = "-fprofile-instr-generate"
        run(
            [
                "make",
                "-C",
                "src",
                f"-j{os.cpu_count() or 4}",
                "valkey-server",
                "valkey-cli",
                "valkey-sentinel",
                "valkey-check-rdb",
                "valkey-check-aof",
            ],
            env=env,
        )
        self.binaries = [
            str(path)
            for path in (
                ROOT / "src" / "valkey-server",
                ROOT / "src" / "valkey-cli",
                ROOT / "src" / "valkey-sentinel",
            )
            if path.exists()
        ]
        if not self.binaries:
            raise CommandError("instrumented binaries were not produced")

    def category_command(self, category: str, tests: list[str]) -> list[str]:
        if category == "main":
            cmd = ["./runtest"]
        elif category == "moduleapi":
            cmd = ["./runtest-moduleapi"]
        elif category == "cluster":
            cmd = ["./runtest-cluster"]
        elif category == "sentinel":
            cmd = ["./runtest-sentinel"]
        else:
            raise ValueError(f"unknown category: {category}")

        for test in tests:
            cmd.extend(["--single", test])
        return cmd

    def covered_sources_for_group(self, category: str, tests: list[str]) -> set[str]:
        key = (category, tuple(tests))
        cached = self.group_cache.get(key)
        if cached is not None:
            return cached

        if not tests:
            self.group_cache[key] = set()
            return set()

        digest = hashlib.sha1(json.dumps(key).encode("utf-8")).hexdigest()[:12]
        profile_dir = TMP_ROOT / digest
        shutil.rmtree(profile_dir, ignore_errors=True)
        profile_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["LLVM_PROFILE_FILE"] = str(profile_dir / "%p.profraw")
        run(self.category_command(category, tests), env=env)

        profraws = sorted(str(path) for path in profile_dir.glob("*.profraw"))
        if not profraws:
            raise CommandError(f"no coverage data produced for {category} group")

        profdata = profile_dir / "coverage.profdata"
        run([self.llvm_profdata, "merge", "-sparse", *profraws, "-o", str(profdata)])
        export = run(
            [
                self.llvm_cov,
                "export",
                "-summary-only",
                "-instr-profile",
                str(profdata),
                *self.binaries,
            ],
            capture_output=True,
        )

        report = json.loads(export.stdout)
        tracked = {str((ROOT / path).resolve()): path for path in self.eligible_sources}
        covered: set[str] = set()
        for chunk in report.get("data", []):
            for file_entry in chunk.get("files", []):
                filename = str(Path(file_entry.get("filename", "")).resolve())
                rel = tracked.get(filename)
                if not rel:
                    continue
                lines = file_entry.get("summary", {}).get("lines", {})
                if lines.get("covered", 0) > 0:
                    covered.add(rel)
                    continue
                for segment in file_entry.get("segments", []):
                    if len(segment) >= 3 and segment[2] > 0:
                        covered.add(rel)
                        break

        self.group_cache[key] = covered
        return covered

    def refine_category(
        self,
        category: str,
        tests: list[str],
        target_sources: set[str],
        assignments: dict[str, set[str]],
    ) -> None:
        active_sources = {src for src in target_sources if len(assignments[src]) < self.max_tests_per_file}
        if not active_sources or not tests:
            return

        covered = self.covered_sources_for_group(category, tests) & active_sources
        if not covered:
            return

        if len(tests) == 1:
            for src in covered:
                assignments[src].add(tests[0])
            return

        if len(tests) <= 4:
            for test in tests:
                self.refine_category(category, [test], covered, assignments)
            return

        midpoint = len(tests) // 2
        left = tests[:midpoint]
        right = tests[midpoint:]
        self.refine_category(category, left, covered, assignments)
        remaining = {src for src in covered if len(assignments[src]) < self.max_tests_per_file}
        self.refine_category(category, right, remaining, assignments)

    def add_assignments(self, category: str, assignments: dict[str, set[str]]) -> None:
        selected = sorted({test for tests in assignments.values() for test in tests})
        if len(selected) > self.max_selected_tests:
            raise CommandError(
                f"{category} selection is too large ({len(selected)} tests), using the full daily workflow is safer"
            )
        if category == "main":
            self.direct_main.update(selected)
        elif category == "moduleapi":
            self.direct_moduleapi.update(selected)
        elif category == "cluster":
            self.direct_cluster.update(selected)
        elif category == "sentinel":
            self.direct_sentinel.update(selected)

    def map_sources(self) -> dict:
        if not self.changed_files:
            return self.build_response(mode="full", reason="no changed files detected")

        for changed in self.changed_files:
            if is_broad_risk(changed):
                return self.build_response(mode="full", reason=f"broad-risk change requires the full daily workflow: {changed}")

        direct_problem = self.collect_direct_tests()
        if direct_problem:
            return self.build_response(mode="full", reason=direct_problem)

        if not self.eligible_sources:
            if self.direct_main or self.direct_moduleapi or self.direct_cluster or self.direct_sentinel:
                return self.build_response(mode="targeted", reason="only direct test-file changes were detected")
            return self.build_response(
                mode="full",
                reason="no source files were eligible for targeted coverage mapping",
            )

        self.build_instrumented_binaries()

        category_tests = {
            "main": self.main_tests,
            "moduleapi": self.moduleapi_tests,
            "cluster": self.cluster_tests,
            "sentinel": self.sentinel_tests,
        }
        category_assignments: dict[str, dict[str, set[str]]] = {
            name: defaultdict(set) for name in category_tests
        }
        category_hits: dict[str, set[str]] = {}

        for category, tests in category_tests.items():
            if not tests:
                category_hits[category] = set()
                continue
            suite_hits = self.covered_sources_for_group(category, tests)
            category_hits[category] = suite_hits
            targets = suite_hits & set(self.eligible_sources)
            if targets:
                self.refine_category(category, tests, targets, category_assignments[category])

        total_hits = set().union(*category_hits.values()) if category_hits else set()
        uncovered = sorted(set(self.eligible_sources) - total_hits)
        if uncovered:
            return self.build_response(
                mode="full",
                reason="some changed source files were not covered by the mapper",
                uncovered=uncovered,
            )

        for category, assignments in category_assignments.items():
            self.add_assignments(category, assignments)

        if not (self.direct_main or self.direct_moduleapi or self.direct_cluster or self.direct_sentinel):
            return self.build_response(
                mode="full",
                reason="the mapper did not find a confident targeted selection",
            )

        return self.build_response(
            mode="targeted",
            reason="targeted test selection was generated from fresh PR-local coverage",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-sha", help="base commit for diff")
    parser.add_argument("--head-sha", default="HEAD", help="head commit for diff")
    parser.add_argument("--changed-file", action="append", default=[], help="override git diff with explicit changed files")
    parser.add_argument("--output", required=True, help="path to write JSON results")
    parser.add_argument("--max-tests-per-file", type=int, default=3)
    parser.add_argument("--max-selected-tests", type=int, default=24)
    parser.add_argument("--skip-build", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.changed_file:
        changed_files = sorted({normalize(path) for path in args.changed_file})
    else:
        if not args.base_sha:
            raise SystemExit("--base-sha is required unless --changed-file is used")
        changed_files = git_changed_files(args.base_sha, args.head_sha)

    mapper = ImpactMapper(
        changed_files,
        max_tests_per_file=args.max_tests_per_file,
        max_selected_tests=args.max_selected_tests,
        skip_build=args.skip_build,
    )
    try:
        result = mapper.map_sources()
    except CommandError as exc:
        result = mapper.build_response(mode="full", reason=str(exc))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
