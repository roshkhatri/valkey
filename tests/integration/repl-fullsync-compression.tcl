# Stage 1: disk-based full-sync streaming compression.
#
# On a disk-based full sync (primary `repl-diskless-sync no`) the primary
# produces ONE shared RDB file per BGSAVE and streams it to every replica that
# attaches to that BGSAVE. The file is LZ4-stream compressed (primary
# `repl-compression lz4-stream`) ONLY when EVERY attaching replica advertised the
# compression capability. A replica advertises that capability when its own
# `repl-compression` is `lz4-stream`.
#
# Decision rules under test:
#   (1) all attaching replicas capable  -> file compressed (VCS header), all load
#   (2) mixed group                      -> file plaintext, ALL replicas load
#   (3) none capable                     -> plaintext (unchanged)
#   (4) primary compression off          -> plaintext even if replica is capable
#
# Non-negotiable correctness property: a non-capable replica must NEVER receive a
# compressed file it cannot load. This is enforced in src by two cooperating
# mechanisms (verified at replication.c):
#   - startBgsaveForReplication ANDs REPLICA_CAPA_COMPRESSION over the whole
#     attaching group, so a single non-capable replica forces plaintext;
#   - syncCommand CASE 1 only lets a newly-arriving replica attach to an
#     in-progress disk BGSAVE when its capabilities are a superset of the
#     triggering replica's, so a non-capable replica can never join a compressed
#     in-flight save -- it waits for its own (plaintext) round.
# Together these hold for every arrival/grouping order, so end-state convergence
# is the property we assert; we additionally assert the on-disk RDB header where
# we can make the grouping deterministic.

# --- helpers --------------------------------------------------------------

proc fsc_read_file_prefix {path count} {
    set fd [open $path r]
    fconfigure $fd -translation binary
    set prefix [read $fd $count]
    close $fd
    return $prefix
}

# Path to the RDB file the primary writes for a disk-based full sync. Requires
# `rdb-del-sync-files no` so it survives for inspection.
proc fsc_dump_path {client} {
    return [file join [lindex [$client config get dir] 1] dump.rdb]
}

# Returns "VCS" when the on-disk RDB is LZ4-stream compressed, otherwise the
# plaintext Valkey magic prefix ("VALKEY").
proc fsc_rdb_is_compressed {client} {
    set header [fsc_read_file_prefix [fsc_dump_path $client] 8]
    return [expr {[string range $header 0 2] eq "VCS"}]
}

# Compressible, multi-type dataset so an LZ4 frame is actually produced.
proc fsc_populate {client prefix {n 300}} {
    $client flushall
    for {set i 0} {$i < $n} {incr i} {
        $client set "${prefix}:str:$i" [string repeat "${prefix}:payload:$i " 16]
    }
    $client rpush "${prefix}:list" a b c d e f g h
    $client sadd "${prefix}:set" alpha beta gamma delta
    $client zadd "${prefix}:zset" 1 one 2 two 3 three 4 four
    $client hset "${prefix}:hash" f1 v1 f2 [string repeat "${prefix}:hashval " 8]
    $client xadd "${prefix}:stream" * f1 s1 f2 [string repeat "${prefix}:streamval " 4]
    $client xadd "${prefix}:stream" * f1 s2 f2 tail
}

# Wait until the replica completes full sync, finishes loading the RDB, and its
# dataset is byte-for-byte identical to the primary (digest + csvdump).
proc fsc_assert_synced {primary replica {tag ""}} {
    wait_for_sync $replica
    wait_done_loading $replica
    wait_for_condition 100 100 {
        [status $replica master_link_status] eq "up" &&
        [$primary debug digest] eq [$replica debug digest]
    } else {
        fail "replica digest mismatch after full sync $tag"
    }
    assert_equal [csvdump $primary] [csvdump $replica] "csvdump mismatch after full sync $tag"
}

# --- case 1: single capable replica -> COMPRESSED + correct ---------------

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set repl-compression lz4-stream
    $primary config set rdb-del-sync-files no
    fsc_populate $primary "cap"

    start_server {overrides {save "" enable-debug-command local repl-compression lz4-stream}} {
        set replica [srv 0 client]

        test {Disk full sync: all-capable group produces a compressed RDB that loads} {
            $replica replicaof $primary_host $primary_port
            # Correctness: replica loaded whatever it received and matches primary.
            fsc_assert_synced $primary $replica "(case1 capable)"

            # Compression-occurred: the shared RDB on disk is LZ4-stream (VCS).
            assert {[file exists [fsc_dump_path $primary]]}
            assert_equal 1 [fsc_rdb_is_compressed $primary]

            # Post-sync incremental write still flows.
            $primary set cap:post "after-sync"
            wait_for_condition 50 100 {
                [$replica get cap:post] eq "after-sync"
            } else {
                fail "post-sync incremental write not received (case1)"
            }

            $replica replicaof no one
        }
    }
}

# --- case 4: primary compression OFF, capable replica -> PLAINTEXT --------

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set repl-compression no
    $primary config set rdb-del-sync-files no
    fsc_populate $primary "off"

    start_server {overrides {save "" enable-debug-command local repl-compression lz4-stream}} {
        set replica [srv 0 client]

        test {Disk full sync: primary compression off yields plaintext even for a capable replica} {
            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(case4 primary-off)"

            # Compression-occurred (negative): file must be plaintext.
            assert_equal 0 [fsc_rdb_is_compressed $primary]

            $replica replicaof no one
        }
    }
}

# --- case 2: single NON-capable replica -> PLAINTEXT + correct ------------

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set repl-compression lz4-stream
    $primary config set rdb-del-sync-files no
    fsc_populate $primary "nocap"

    start_server {overrides {save "" enable-debug-command local repl-compression no}} {
        set replica [srv 0 client]

        test {Disk full sync: a non-capable replica gets a plaintext RDB and loads it} {
            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(case2 non-capable)"

            # Safety: the file the non-capable replica consumed is plaintext.
            assert_equal 0 [fsc_rdb_is_compressed $primary]

            $replica replicaof no one
        }
    }
}

# --- case 3a: mixed group, end-state correctness (robust, grouping-agnostic)

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set repl-compression lz4-stream
    $primary config set rdb-del-sync-files no
    fsc_populate $primary "mix"

    start_server {overrides {save "" enable-debug-command local repl-compression lz4-stream}} {
        set capable [srv 0 client]

        start_server {overrides {save "" enable-debug-command local repl-compression no}} {
            set noncap [srv 0 client]

            test {Disk full sync: mixed capable/non-capable replicas both converge correctly} {
                # Attach both to the same primary. Whether they land in one BGSAVE
                # group or separate rounds is timing-dependent, but the safety
                # property (a non-capable replica never receives an unloadable
                # compressed file) holds for every order: the capa-superset attach
                # guard keeps the non-capable replica off any compressed in-flight
                # save, and the AND-over-group rule downgrades a shared group to
                # plaintext. Either way BOTH must end up correctly synced.
                $capable replicaof $primary_host $primary_port
                $noncap replicaof $primary_host $primary_port

                fsc_assert_synced $primary $capable "(case3a capable)"
                fsc_assert_synced $primary $noncap   "(case3a non-capable)"

                $noncap replicaof no one
                $capable replicaof no one
            }
        }
    }
}

# --- case 3b: mixed group, DETERMINISTIC single-BGSAVE grouping -----------
#
# Force both replicas into ONE disk BGSAVE group so the AND-over-group rule is
# exercised directly: while a slow manual BGSAVE child is running, a disk replica
# cannot start its own BGSAVE (syncCommand defers to replicationCron), so both
# replicas park in WAIT_BGSAVE_START. When the manual child finishes,
# replicationStartPendingFork groups every waiting replica into a single BGSAVE
# whose capability is the AND across the group -> one non-capable member forces a
# plaintext shared file that both replicas load.
#
# This relies on the active-child deferral path; it is deterministic given a
# manual BGSAVE that is still running when both replicas register (asserted via
# connected_slaves==2 before the child completes). The single-grouped-round
# expectation is asserted via the sync_full delta; if the environment somehow
# splits the rounds the test fails loudly rather than asserting a wrong header.

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set repl-compression lz4-stream
    $primary config set rdb-del-sync-files no
    # Enough keys + per-key delay so the manual BGSAVE child stays alive long
    # enough for both replicas to register in WAIT_BGSAVE_START.
    fsc_populate $primary "grp" 800

    start_server {overrides {save "" enable-debug-command local repl-compression lz4-stream}} {
        set capable [srv 0 client]

        start_server {overrides {save "" enable-debug-command local repl-compression no}} {
            set noncap [srv 0 client]

            test {Disk full sync: a single grouped BGSAVE with a non-capable member is plaintext for all} {
                # Count replication BGSAVE rounds via the primary log: the primary
                # logs "Starting BGSAVE for SYNC" exactly once per round, so a
                # grouped sync of both replicas yields a delta of 1 (srv -2 is the
                # primary inside this doubly-nested scope).
                set rounds_before [count_log_message -2 {Starting BGSAVE for SYNC}]

                # Start a slow manual BGSAVE to occupy the RDB child slot.
                $primary config set rdb-key-save-delay 5000
                assert_match {*Background saving started*} [$primary bgsave]
                wait_for_condition 200 10 {
                    [status $primary rdb_bgsave_in_progress] eq 1
                } else {
                    $primary config set rdb-key-save-delay 0
                    fail "manual BGSAVE did not start"
                }

                # Register both replicas while the manual child is still running.
                # They cannot trigger their own BGSAVE, so both park in
                # WAIT_BGSAVE_START (counted by connected_slaves).
                $capable replicaof $primary_host $primary_port
                $noncap replicaof $primary_host $primary_port
                wait_for_condition 200 10 {
                    [status $primary connected_slaves] == 2 &&
                    [status $primary rdb_bgsave_in_progress] eq 1
                } else {
                    $primary config set rdb-key-save-delay 0
                    fail "both replicas did not register before manual BGSAVE finished (grouping not achieved)"
                }

                # Let the grouped replication BGSAVE run fast once the manual
                # child clears.
                $primary config set rdb-key-save-delay 0

                # Resetting the delay in the parent does not speed up the
                # already-forked manual child, so the grouped replication BGSAVE
                # only starts once that child exits. Wait for the replication
                # round to actually begin before asserting sync, decoupling the
                # slow manual child from the sync-wait window.
                wait_for_condition 200 50 {
                    ([count_log_message -2 {Starting BGSAVE for SYNC}] - $rounds_before) >= 1
                } else {
                    fail "grouped replication BGSAVE round did not start"
                }

                fsc_assert_synced $primary $capable "(case3b capable, grouped)"
                fsc_assert_synced $primary $noncap   "(case3b non-capable, grouped)"

                # Exactly one full-sync BGSAVE served both replicas -> they were
                # grouped (the precondition for the AND rule to apply).
                assert_equal 1 [expr {[count_log_message -2 {Starting BGSAVE for SYNC}] - $rounds_before}]

                # AND-rule result: the shared grouped RDB is plaintext, so the
                # capable replica was downgraded alongside the non-capable one.
                assert_equal 0 [fsc_rdb_is_compressed $primary]

                $noncap replicaof no one
                $capable replicaof no one
            }
        }
    }
}

# ============================================================================
# Full-sync streaming compression: cross-cutting transfer-path coverage.
# Exercises the disk-receive (BIO) load path and dual-channel, complementing the
# disk-grouping cases above and repl-compression.tcl's diskless-load coverage.
# Requires `repl-compression lz4-stream` on both primary and replica.
# ============================================================================

tags {"repl external:skip"} {

# 1. Dual-channel + compression: replica receives the RDB to disk (BIO) and
#    rdbLoad() auto-decompresses.

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0 dual-channel-replication-enabled yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Dual-channel + compression: full sync delivers identical data} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary "dc"

        start_server {overrides {save "" repl-compression lz4-stream dual-channel-replication-enabled yes}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "(dual-channel)"

            # Primary's compression log + replica's load log confirm compression occurred.
            wait_for_log_messages -1 {"*using: dual-channel*"} $primary_loglines 50 100
            wait_for_log_messages -1 {"*diskless full sync with compression: lz4*"} $primary_loglines 50 100
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100

            $replica replicaof no one
        }
    }
}

# 2. Diskless compressed save + default repl-diskless-load (disabled): replica
#    copies the payload to a temp file, then rdbLoad() auto-decompresses.

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Disk-receive (repl-diskless-load disabled) + diskless compressed save loads correctly} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary "diskrecv"

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load disabled}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "(disk-receive default load)"

            wait_for_log_messages -1 {"*target: replicas sockets*"} $primary_loglines 50 100
            wait_for_log_messages -1 {"*diskless full sync with compression: lz4*"} $primary_loglines 50 100
            # Path B: load log reads "from <filename>", not "from primary".
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100

            $replica replicaof no one
        }
    }
}

# 3. Checksum interaction: a compressed diskless full sync loads and matches
#    under both rdbchecksum yes and no. (rdbchecksum is immutable, set via
#    startup override; the TLS-gated checksum-skip negotiation is not exercised.)

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Compressed diskless full sync loads with default checksum settings} {
        fsc_populate $primary "cksum-on"
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load disabled}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(checksum on)"
            $replica replicaof no one
        }
    }
}

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0 rdbchecksum no}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Compressed diskless full sync loads with rdbchecksum no on the primary} {
        # Confirm the immutable override actually took effect.
        assert_equal "no" [lindex [$primary config get rdbchecksum] 1]
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary "cksum-off"

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load disabled}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "(checksum off)"

            wait_for_log_messages -1 {"*diskless full sync with compression: lz4*"} $primary_loglines 50 100
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100

            $replica replicaof no one
        }
    }
}

# 4. Full sync -> incremental handoff: after a compressed full sync, post-sync
#    writes replicate over the compressed incremental stream (both load paths).

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Compressed full sync -> compressed incremental handoff (diskless-load replica)} {
        fsc_populate $primary "inc-load"

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(incremental, diskless-load)"

            # Wait until compression is active on the post-sync link.
            wait_for_condition 50 200 {
                [string match {*state=online*compression=lz4*} [$primary info replication]]
            } else {
                fail "compression not active on diskless-load replica after full sync"
            }

            # Writes after full sync go through the compressed incremental stream.
            for {set i 0} {$i < 200} {incr i} {
                $primary set "inc-load:after:$i" [string repeat "delta$i " 12]
            }
            $primary rpush "inc-load:after:list" x y z
            wait_for_condition 50 200 {
                [$replica get "inc-load:after:199"] eq [string repeat "delta199 " 12]
            } else {
                fail "post-full-sync incremental writes not received (diskless-load)"
            }
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal [csvdump $primary] [csvdump $replica]

            $replica replicaof no one
        }
    }

    test {Compressed full sync -> compressed incremental handoff (disk-receive replica)} {
        fsc_populate $primary "inc-disk"

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load disabled}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(incremental, disk-receive)"

            wait_for_condition 50 200 {
                [string match {*state=online*compression=lz4*} [$primary info replication]]
            } else {
                fail "compression not active on disk-receive replica after full sync"
            }

            for {set i 0} {$i < 200} {incr i} {
                $primary set "inc-disk:after:$i" [string repeat "delta$i " 12]
            }
            $primary hset "inc-disk:after:hash" a 1 b 2 c 3
            wait_for_condition 50 200 {
                [$replica get "inc-disk:after:199"] eq [string repeat "delta199 " 12]
            } else {
                fail "post-full-sync incremental writes not received (disk-receive)"
            }
            assert_equal [$primary debug digest] [$replica debug digest]
            assert_equal [csvdump $primary] [csvdump $replica]

            $replica replicaof no one
        }
    }
}

# 5. Teardown + resync: REPLICAOF NO ONE after a compressed full sync, then
#    re-attach and require a second clean compressed full sync. Guards against
#    stale decompressor/stream-reader state leaking across syncs. (Mid-sync abort
#    + corrupt-stream injection is left as a follow-up; no debug hook exists to
#    deterministically corrupt a payload mid-frame.)

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {REPLICAOF NO ONE after a compressed full sync, then resync, still works} {
        fsc_populate $primary "teardown"

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]

            # First compressed full sync.
            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(teardown: first sync)"

            # Promote to standalone: tears down replication + decompressor state.
            $replica replicaof no one
            wait_for_condition 50 100 {
                [s 0 role] eq "master"
            } else {
                fail "replica did not become master after REPLICAOF NO ONE"
            }

            # Change the dataset so the resync ships different keys (forces a real reload).
            fsc_populate $primary "teardown2"

            # Re-attach: a second clean compressed full sync; stale state would corrupt it.
            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(teardown: resync)"

            # The post-resync incremental stream (also compressed) still flows.
            $primary set teardown2:post "after-resync"
            wait_for_condition 50 100 {
                [$replica get teardown2:post] eq "after-resync"
            } else {
                fail "post-resync incremental write not received"
            }

            $replica replicaof no one
        }
    }
}

# 6. Disk-based master (repl-diskless-sync no) + diskless-load replica: the
#    primary BGSAVEs a compressed RDB to disk and streams it with size framing
#    ($<len>, no EOF mark), and the replica loads it directly from the socket via
#    replicaLoadPrimaryRDBFromSocket. Regression for size-framed transfers whose
#    decompression was gated on the EOF mark (usemark): decompression was skipped
#    and the VCS frame was read as a raw RDB ("Wrong signature ... VCS"), causing
#    an infinite resync loop.

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync no rdb-del-sync-files no}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Disk-based compressed full sync is decompressed on a diskless-load (swapdb) replica} {
        fsc_populate $primary "diskmaster"

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "(disk-master, diskless-load swapdb)"

            # The disk BGSAVE produced a compressed (VCS) RDB on the primary...
            assert_equal 1 [fsc_rdb_is_compressed $primary]
            # ...and the replica decompressed it on the size-framed socket path
            # (the log line is only emitted by replicaLoadPrimaryRDBFromSocket).
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100

            # Post-sync incremental writes still flow over the compressed stream.
            $primary set diskmaster:post "after-sync"
            wait_for_condition 50 100 {
                [$replica get diskmaster:post] eq "after-sync"
            } else {
                fail "post-sync incremental write not received (disk-master, diskless-load)"
            }

            $replica replicaof no one
        }
    }
}

# 7. Mid-transfer link drop on a compressed diskless full sync: the replica's
#    inline socket decompressor (repl-diskless-load swapdb -> the
#    replicaLoadPrimaryRDBFromSocket TRUNCATED path) sees a clean EOF before the
#    LZ4 frame end. That is recoverable truncation, NOT codec corruption, so the
#    replica must survive, retry, and complete a clean resync rather than abort.
#    This is the integration counterpart of the unit test
#    streamReaderRejectsTruncatedFrameTrailer (clean EOF mid-frame -> TRUNCATED).
#
#    Determinism: compression makes the diskless child finish almost instantly,
#    leaving no window to interrupt. We stretch it with a per-key save delay over
#    a sizable compressible dataset so the compressed stream flows slowly. The
#    replica loads the RDB inline with a *blocking* socket read, so polling its
#    INFO is unreliable mid-load; instead we watch the replica's log for the
#    "Loading compressed RDB" line (filesystem poll, independent of the blocked
#    replica) which marks the moment the inline decoder has begun the frame, and
#    drop the link in the wide (~seconds) window before the load completes. After
#    the kill we reset the delay so the retry resync runs fast.

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Compressed diskless full sync recovers from a mid-transfer link drop (truncation, not corruption)} {
        # Sizable compressible dataset so the stretched diskless child streams the
        # compressed RDB slowly enough to interrupt mid-frame.
        fsc_populate $primary "trunc" 2000
        set sync_full_before [status $primary sync_full]

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]

            # Stretch the diskless RDB child (5ms/key over ~2000 keys) so the
            # compressed stream stays in flight long enough to interrupt.
            $primary config set rdb-key-save-delay 5000

            $replica replicaof $primary_host $primary_port

            # Catch the replica once it has begun decoding the compressed RDB
            # inline from the socket -- the TRUNCATED decode path under test. The
            # log line is emitted by replicaLoadPrimaryRDBFromSocket at the start
            # of the inline decode; watching the log avoids polling the replica,
            # which is blocked in the synchronous socket read during the load.
            wait_for_log_messages 0 {"*Loading compressed RDB*"} $replica_loglines 300 100

            # Drop the link mid-frame: kill the in-flight compressed transfer so
            # the replica's stream reader hits a clean EOF before the frame end.
            # The ~5s remaining load window dwarfs the detect-and-kill latency.
            $primary client kill type replica

            # Let the retry resync run fast.
            $primary config set rdb-key-save-delay 0

            # Recovery (1): the replica process survived the truncated load.
            assert_equal {PONG} [$replica ping]

            # Recovery (2): it retries and converges on a clean, complete resync.
            # Use a generous window: reconnect is driven by the replication cron
            # (~1s) plus the fast retry sync.
            wait_for_condition 300 100 {
                [status $replica master_link_status] eq "up"
            } else {
                fail "replica did not recover the link after a mid-transfer drop"
            }
            fsc_assert_synced $primary $replica "(truncation recovery)"

            # A second (clean) full sync happened after the aborted one.
            assert {[status $primary sync_full] > $sync_full_before}

            # The interrupted transfer exercised the inline compressed-decode path
            # and was handled as a recoverable load failure -- not a crash or a
            # corruption abort. (`ping` above already proves the server is alive;
            # this also confirms there was no panic/assertion.)
            assert {[count_log_message 0 "Loading compressed RDB"] >= 1}
            assert {[count_log_message 0 "Failed trying to load the PRIMARY synchronization DB"] >= 1}
            assert_equal 0 [count_log_message 0 "=== ASSERTION FAILED ==="]
            # The recovery resync logged a clean success.
            assert {[count_log_message 0 "REPLICA sync: Finished with success"] >= 1}

            # Post-recovery incremental writes still flow over the compressed link.
            $primary set trunc:post "after-recovery"
            wait_for_condition 50 100 {
                [$replica get trunc:post] eq "after-recovery"
            } else {
                fail "post-recovery incremental write not received"
            }

            $replica replicaof no one
        }
    }
}

} ;# end tags
