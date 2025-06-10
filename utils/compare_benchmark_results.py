#!/usr/bin/env python3
import json
import statistics
import sys


if len(sys.argv) < 3:
    print("Usage: compare_benchmark_results.py BASELINE NEW [OUT_FILE]", file=sys.stderr)
    sys.exit(1)

baseline_file = sys.argv[1]
new_file = sys.argv[2]
out_file = sys.argv[3] if len(sys.argv) > 3 else None


def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _mean(values):
    values = [v for v in values if v is not None]
    return statistics.mean(values) if values else 0.0


def summarize(data):
    if isinstance(data, list):
        def field(name, alt=None):
            return _mean([d.get(name) if d.get(name) is not None else d.get(alt) for d in data])
    else:
        def field(name, alt=None):
            return data.get(name, data.get(alt, 0.0))

    return {
        "rps": field("rps"),
        "latency_avg_ms": field("avg_latency_ms", "latency_avg_ms"),
        "latency_p50_ms": field("p50_latency_ms", "latency_p50_ms"),
        "latency_p95_ms": field("p95_latency_ms", "latency_p95_ms"),
        "latency_p99_ms": field("p99_latency_ms", "latency_p99_ms"),
    }


baseline = summarize(load(baseline_file))
new = summarize(load(new_file))

metrics = [
    "rps",
    "latency_avg_ms",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
]


def pct_change(new_v, old_v):
    return ((new_v - old_v) / old_v * 100.0) if old_v else 0.0


lines = ["| Metric | Baseline | PR | Diff | % Change |", "| --- | --- | --- | --- | --- |"]
for m in metrics:
    b = baseline.get(m, 0.0)
    n = new.get(m, 0.0)
    diff = n - b
    change = pct_change(n, b)
    lines.append(f"| {m} | {b:.2f} | {n:.2f} | {diff:.2f} | {change:+.2f}% |")


table = "\n".join(lines)

if out_file:
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(table)
print(table)

