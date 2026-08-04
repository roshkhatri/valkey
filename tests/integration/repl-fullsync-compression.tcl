# Full-sync streaming compression: end-to-end coverage.
#
# Scope: disk-based full sync, diskless (replicas-sockets) full sync,
# dual-channel full sync, both replica-side load paths (receive-to-disk and
# direct socket load), negative cases (truncation, corruption, premature EOF,
# non-capable cohorts), and network byte accounting under compression. Server
# fixtures are shared across compatible tests; each test re-captures its own log
# offset and repopulates its own key prefix so a shared primary stays neutral.
#
# Decision rules under test (asymmetric policy):
#   (1) all attaching replicas capable  -> compressed round (VCS header), all load
#   (2) mixed group                      -> single plaintext round for all (the
#                                           group-AND clears the compress bit); a
#                                           capable replica may receive plaintext
#   (3) none capable                     -> plaintext (unchanged)
#   (4) primary compression off          -> plaintext even if replica is capable
#
# Non-negotiable correctness property: a non-capable replica must NEVER receive a
# compressed file it cannot load. A capable replica MAY receive plaintext.

# --- helpers --------------------------------------------------------------

# Path to the RDB file the primary writes for a disk-based full sync. Requires
# `rdb-del-sync-files no` so it survives for inspection.
proc fsc_dump_path {client} {
    return [file join [lindex [$client config get dir] 1] dump.rdb]
}

# Returns 1 when the on-disk RDB is LZ4-stream compressed (VCS magic), else 0.
proc fsc_rdb_is_compressed {client} {
    set header [read_binary_file_prefix [fsc_dump_path $client] 8]
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

# Wait (generous 300x100 budget for slow/shared primaries, also the macOS timing
# fix for the delay-based piggyback tests) for the replica link to come up, then
# assert its dataset digest matches the primary.
proc fsc_assert_synced {primary replica {tag ""}} {
    wait_for_condition 300 100 {
        [status $replica master_link_status] eq "up"
    } else {
        fail "replica link not up after full sync $tag"
    }
    assert_equal [$primary debug digest] [$replica debug digest] "replica digest mismatch after full sync $tag"
}

# Start a single-shot fake primary (tests/helpers/fake_primary.tcl) on a free
# port. It answers the handshake, announces $announce bytes, sends $payload,
# then closes. Returns {pid port}.
proc fsc_start_fake_primary {payload announce} {
    set payload_file [tmpfile fsc-fake-primary-payload]
    write_binary_file $payload_file $payload
    set port [find_available_port $::baseport $::portcount]
    set pid [exec [info nameofexecutable] tests/helpers/fake_primary.tcl $port $payload_file $announce &]
    wait_for_condition 50 50 {
        [catch {close [socket 127.0.0.1 $port]}] == 0
    } else {
        fail "Failed to start fake primary"
    }
    return [list $pid $port]
}

# ============================================================================
# Disk-based full sync: ONE shared primary fixture.
#
# rdbcompression is MODIFIABLE and applies to subsequent syncs, so each test
# sets the value it needs at its start (lz4 or yes) and repopulates its own key
# prefix (flushall inside fsc_populate resets the dataset). The compression-off
# test restores the fixture default (lz4) at its end. Replicas are per-test
# nested start_servers because their capability configs differ. The
# restart-from-snapshot test is NOT part of this group (it restarts its primary)
# and stays isolated below.
# ============================================================================

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set rdbcompression lz4
    $primary config set rdb-del-sync-files no

    # --- case 1: single capable replica -> COMPRESSED + correct -----------
    start_server {overrides {save "" enable-debug-command local rdbcompression lz4}} {
        set replica [srv 0 client]

        test {Disk full sync: all-capable group produces a compressed RDB that loads} {
            $primary config set rdbcompression lz4
            fsc_populate $primary "cap"
            set primary_loglines [count_log_lines -1]

            $replica replicaof $primary_host $primary_port
            # Correctness: replica loaded whatever it received and matches primary.
            fsc_assert_synced $primary $replica "(case1 capable)"

            # Compression-occurred: the shared RDB on disk is LZ4-stream (VCS).
            assert {[file exists [fsc_dump_path $primary]]}
            assert_equal 1 [fsc_rdb_is_compressed $primary]
            wait_for_log_messages -1 {"*Disk-based full sync with compression: lz4*"} $primary_loglines 50 100

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

    # --- plaintext negotiation: primary-off and replica-non-capable -------
    # Both scenarios must fall back to a plaintext RDB. The replica's advertised
    # compress-sync capability follows its own rdbcompression, set per
    # iteration; the primary's mode is set per iteration too.
    start_server {overrides {save "" enable-debug-command local rdbcompression lz4}} {
        set replica [srv 0 client]

        foreach {scenario primary_mode replica_mode prefix} {
            primary-compression-off yes lz4 off
            replica-not-capable     lz4 yes nocap
        } {
            test "Disk full sync: $scenario yields a plaintext RDB that loads" {
                $primary config set rdbcompression $primary_mode
                $replica config set rdbcompression $replica_mode
                fsc_populate $primary $prefix

                $replica replicaof $primary_host $primary_port
                fsc_assert_synced $primary $replica "($scenario)"

                # Negotiation fell back to plaintext, so the shared RDB is plaintext.
                assert_equal 0 [fsc_rdb_is_compressed $primary]

                $replica replicaof no one
            }
        }
        # Restore the fixture default for subsequent shared-primary tests.
        $primary config set rdbcompression lz4
    }

    # --- disk-master, diskless-load: size-framed compressed direct load ---
    # The primary BGSAVEs a compressed RDB to disk and streams it with size
    # framing ($<len>, no EOF mark); the replica loads it directly from the
    # socket via replicaLoadPrimaryRDBFromSocket. Regression for size-framed
    # transfers whose decompression was gated on the EOF mark (usemark):
    # decompression was skipped and the VCS frame was read as a raw RDB ("Wrong
    # signature ... VCS"), causing an infinite resync loop.
    start_server {overrides {save "" enable-debug-command local rdbcompression lz4 repl-diskless-load swapdb}} {
        set replica [srv 0 client]

        test {Disk-based compressed full sync is decompressed on a diskless-load (swapdb) replica} {
            $primary config set rdbcompression lz4
            fsc_populate $primary "diskmaster"
            set replica_loglines [count_log_lines 0]

            $replica replicaof $primary_host $primary_port
            fsc_assert_synced $primary $replica "(disk-master, diskless-load swapdb)"

            # The disk BGSAVE produced a compressed (VCS) RDB on the primary...
            assert_equal 1 [fsc_rdb_is_compressed $primary]
            # ...and the replica decompressed it on the size-framed socket path
            # (the log line is only emitted by replicaLoadPrimaryRDBFromSocket).
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100

            # Post-sync incremental writes still flow.
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

# --- case 3b: mixed disk group, DETERMINISTIC simultaneous parking --------
#
# Force both replicas to park in WAIT_BGSAVE_START at the same time so the
# group-AND decision is exercised directly: while a slow manual BGSAVE child is
# running, a disk replica cannot start its own BGSAVE (syncCommand defers to
# replicationCron), so both replicas park. When the manual child finishes, the
# cron groups both waiters and the mincapa AND over the group clears the
# compress-sync bit (the non-capable member drops it), so a single plaintext
# round serves everyone and no replica receives a format it cannot load.
#
# This relies on the active-child deferral path; it is deterministic given a
# manual BGSAVE that is still running when both replicas register (asserted via
# connected_slaves==2 before the child completes). The single-round expectation
# is asserted via the "Starting BGSAVE for SYNC" delta.

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set rdbcompression lz4
    $primary config set rdb-del-sync-files no
    # Enough keys + per-key delay so the manual BGSAVE child stays alive long
    # enough for both replicas to register in WAIT_BGSAVE_START.
    fsc_populate $primary "grp" 800

    start_server {overrides {save "" enable-debug-command local rdbcompression lz4}} {
        set capable [srv 0 client]

        start_server {overrides {save "" enable-debug-command local rdbcompression yes}} {
            set noncap [srv 0 client]

            test {Disk full sync: a single grouped BGSAVE with a non-capable member is plaintext for all} {
                # Count replication BGSAVE rounds via the primary log: the primary
                # logs "Starting BGSAVE for SYNC" exactly once per round (srv -2 is
                # the primary inside this doubly-nested scope).
                set rounds_before [count_log_message -2 {Starting BGSAVE for SYNC}]
                set primary_loglines [count_log_lines -2]

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

                fsc_assert_synced $primary $capable "(case3b capable, grouped)"
                fsc_assert_synced $primary $noncap   "(case3b non-capable, grouped)"

                # Exactly one full-sync BGSAVE served both replicas -> they were
                # grouped (the precondition for the AND rule to apply).
                assert_equal 1 [expr {[count_log_message -2 {Starting BGSAVE for SYNC}] - $rounds_before}]

                # AND-rule result: the shared grouped RDB is plaintext, so the
                # capable replica was downgraded alongside the non-capable one.
                assert_equal 0 [fsc_rdb_is_compressed $primary]
                verify_no_log_message -2 "*Disk-based full sync with compression: lz4*" $primary_loglines

                $noncap replicaof no one
                $capable replicaof no one
            }
        }
    }
}

# ============================================================================
# Piggyback (in-flight BGSAVE join) coverage: ONE shared primary fixture.
#
# Each test sets its own rdbcompression via CONFIG SET, repopulates its own
# key prefix, re-captures its own log offset, and opens the in-flight window with
# rdb-key-save-delay 13000 (~2s over 155 keys x 13ms: above one PSYNC round trip,
# under wait_for_sync's ~5s budget on slow runners), closing it with delay 0 at
# the end. Replicas stay per-test nested start_servers because their capability
# configs differ.
# ============================================================================

start_server {tags {"repl rdb-compression external:skip needs:debug"} overrides {save "" enable-debug-command local}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    $primary config set repl-diskless-sync no
    $primary config set rdb-del-sync-files no

    start_server {overrides {save "" enable-debug-command local rdbcompression lz4}} {
        set replica_capable [srv 0 client]

        start_server {overrides {save "" enable-debug-command local rdbcompression yes}} {
            set replica_plain [srv 0 client]

            test {Plain disk BGSAVE in flight is joinable by a non-capable replica} {
                $primary config set rdbcompression yes
                $primary config set rdb-key-save-delay 13000
                fsc_populate $primary "piggy" 150
                set primary_loglines [count_log_lines -2]

                $replica_capable replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Starting BGSAVE for SYNC with target: disk*"} $primary_loglines 50 100

                # Arrives mid-save: attaches to the running plain save even
                # though the save was started for a compress-sync replica.
                $replica_plain replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Waiting for end of BGSAVE for SYNC*"} $primary_loglines 50 100
                verify_no_log_message -2 "*Waiting for next BGSAVE for SYNC*" $primary_loglines

                $primary config set rdb-key-save-delay 0
                fsc_assert_synced $primary $replica_capable "(piggyback plain, capable)"
                fsc_assert_synced $primary $replica_plain "(piggyback plain, non-capable)"

                $replica_plain replicaof no one
                $replica_capable replicaof no one
            }
        }
    }

    start_server {overrides {save "" enable-debug-command local rdbcompression lz4}} {
        set replica_capable [srv 0 client]

        start_server {overrides {save "" enable-debug-command local rdbcompression yes}} {
            set replica_plain [srv 0 client]

            test {Compressed disk BGSAVE in flight is not joinable by a non-capable replica} {
                $primary config set rdbcompression lz4
                $primary config set rdb-key-save-delay 13000
                fsc_populate $primary "piggyc" 150
                set primary_loglines [count_log_lines -2]

                $replica_capable replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Disk-based full sync with compression: lz4*"} $primary_loglines 50 100

                # Arrives mid-save: must wait for the next BGSAVE, since the
                # running save is compressed and it cannot load that format.
                $replica_plain replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Can't attach the replica to the current BGSAVE*"} $primary_loglines 50 100

                $primary config set rdb-key-save-delay 0
                fsc_assert_synced $primary $replica_capable "(piggyback compressed, capable)"
                fsc_assert_synced $primary $replica_plain "(piggyback compressed, non-capable)"

                # The second save, for the non-capable cohort, is plaintext.
                assert_equal 0 [fsc_rdb_is_compressed $primary]

                $replica_plain replicaof no one
                $replica_capable replicaof no one
            }
        }
    }

    start_server {overrides {save "" enable-debug-command local rdbcompression lz4}} {
        set replica_capable [srv 0 client]

        start_server {overrides {save "" enable-debug-command local rdbcompression lz4}} {
            set replica_capable2 [srv 0 client]

            test {Compressed disk BGSAVE in flight is joinable by a capable replica} {
                $primary config set rdbcompression lz4
                $primary config set rdb-key-save-delay 13000
                fsc_populate $primary "piggya" 150
                set primary_loglines [count_log_lines -2]

                $replica_capable replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Disk-based full sync with compression: lz4*"} $primary_loglines 50 100

                # Arrives mid-save: capable of the running save's format, so it
                # attaches to the current compressed BGSAVE.
                $replica_capable2 replicaof $primary_host $primary_port
                wait_for_log_messages -2 {"*Waiting for end of BGSAVE for SYNC*"} $primary_loglines 50 100
                verify_no_log_message -2 "*Can't attach the replica to the current BGSAVE*" $primary_loglines

                $primary config set rdb-key-save-delay 0
                fsc_assert_synced $primary $replica_capable "(piggyback attach, trigger)"
                fsc_assert_synced $primary $replica_capable2 "(piggyback attach, joiner)"

                # The single save that served both replicas stayed compressed.
                assert_equal 1 [fsc_rdb_is_compressed $primary]

                $replica_capable2 replicaof no one
                $replica_capable replicaof no one
            }
        }
    }

}

# ============================================================================
# Full-sync streaming compression: cross-cutting transfer-path coverage.
# Exercises the disk-receive (BIO) load path, dual-channel, diskless payload
# compression, and network byte-accounting under compression.
# Requires `rdbcompression lz4` on both primary and replica.
# ============================================================================

tags {"repl external:skip"} {

# --- dual-channel: ONE shared primary fixture ------------------------------
#
# dual-channel-replication-enabled yes, repl-diskless-sync yes, rdbcompression
# lz4, save "". The delay-0 receive-to-disk and direct-socket-load tests share it
# directly; the two-replica byte-accounting test raises repl-diskless-sync-delay
# and repl-diskless-sync-max-replicas via CONFIG SET (both MODIFIABLE) at its
# start and restores them at its end. Each test recaptures its own log offset and
# repopulates its own prefix.

start_server {overrides {save "" rdbcompression lz4 repl-diskless-sync yes repl-diskless-sync-delay 0 dual-channel-replication-enabled yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # Dual-channel + compression success across both replica load paths:
    #   disabled -> replica receives the RDB to disk (BIO), rdbLoad() decompresses;
    #   swapdb   -> replica loads the compressed payload directly from the socket.
    # swapdb also exercises the capa-compression advertising in the dual-channel
    # handshake.
    foreach load_mode {disabled swapdb} {
        test "Dual-channel + compression full sync delivers identical data (repl-diskless-load $load_mode)" {
            set primary_loglines [count_log_lines 0]
            fsc_populate $primary "dc-$load_mode"

            start_server [list overrides [list save "" rdbcompression lz4 repl-diskless-load $load_mode dual-channel-replication-enabled yes]] {
                set replica [srv 0 client]
                set replica_loglines [count_log_lines 0]
                $replica replicaof $primary_host $primary_port

                fsc_assert_synced $primary $replica "(dual-channel $load_mode)"

                # Primary logs confirm a compressed dual-channel diskless round.
                wait_for_log_messages -1 {"*using: dual-channel*"} $primary_loglines 50 100
                wait_for_log_messages -1 {"*Diskless full sync with compression: lz4*"} $primary_loglines 50 100
                # swapdb loads inline from the socket ("from primary"); disabled loads from the received file.
                if {$load_mode eq "swapdb"} {
                    wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4) from primary*"} $replica_loglines 50 100
                } else {
                    wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100
                }

                $replica replicaof no one
            }
        }
    }

    # Aggregate output accounting across all dual-channel sockets: one BGSAVE that
    # serves TWO dual-channel replicas writes the compressed payload to both
    # sockets from a single connset. The primary must count the bytes sent to
    # every socket, so total_net_repl_output_bytes covers both replicas' received
    # bytes. The old single-stream accounting reported one stream (~half the sum)
    # and fails the >=75% check below. This test needs the delay window and a
    # 2-replica cap; both are set here and restored at the end.
    test {Dual-channel diskless full sync counts output bytes across all sockets} {
        $primary config set repl-diskless-sync-delay 1000
        $primary config set repl-diskless-sync-max-replicas 2
        fsc_populate $primary agg 2000

        start_server {overrides {save "" rdbcompression lz4 repl-diskless-load swapdb dual-channel-replication-enabled yes}} {
            set replica1 [srv 0 client]

            start_server {overrides {save "" rdbcompression lz4 repl-diskless-load swapdb dual-channel-replication-enabled yes}} {
                set replica2 [srv 0 client]

                # Fresh accounting, then attach both within the delay window so a
                # single BGSAVE groups both RDB channels into one connset.
                set rounds_before [count_log_message -2 {Starting BGSAVE for SYNC}]
                $primary config resetstat
                $replica1 replicaof $primary_host $primary_port
                $replica2 replicaof $primary_host $primary_port

                fsc_assert_synced $primary $replica1 "(agg r1)"
                fsc_assert_synced $primary $replica2 "(agg r2)"

                # Exactly one grouped BGSAVE served both replicas: the precondition
                # for the aggregation (single connset, two sockets) to be exercised.
                assert_equal 1 [expr {[count_log_message -2 {Starting BGSAVE for SYNC}] - $rounds_before}]

                # The child reports its per-connset output bytes to the parent
                # over the child-info pipe, which the parent drains asynchronously
                # after the replicas report link-up. Wait for that aggregation to
                # land before sampling the counters.
                wait_for_condition 50 100 {
                    [status $primary total_net_repl_output_bytes] > 0
                } else {
                    fail "primary did not aggregate dual-channel output bytes"
                }

                set out [status $primary total_net_repl_output_bytes]
                set in1 [status $replica1 total_net_repl_input_bytes]
                set in2 [status $replica2 total_net_repl_input_bytes]
                set total_in [expr {$in1 + $in2}]

                assert_morethan $out 0
                assert_morethan $in1 0
                assert_morethan $in2 0
                # Aggregated output covers both sockets; old single-stream
                # accounting reported ~half and fails the lower bound. The upper
                # bound guards against double-counting.
                assert {$out >= $total_in * 0.75}
                assert {$out <= $total_in * 1.25}

                $replica2 replicaof no one
                $replica1 replicaof no one
            }
        }

        # Restore the fixture defaults for the shared dual-channel primary.
        $primary config set repl-diskless-sync-delay 0
        $primary config set repl-diskless-sync-max-replicas 0
    }
}

# --- ordinary diskless: ONE shared primary fixture -------------------------
#
# repl-diskless-sync yes, repl-diskless-sync-delay 0, rdbcompression lz4,
# save "". Shared by the receive-to-disk (repl-diskless-load disabled) test and
# the direct-socket-load (swapdb) test; replicas differ only in repl-diskless-load
# so they stay per-test. Each test recaptures its own log offset and prefix.

start_server {overrides {save "" rdbcompression lz4 repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # 2. Diskless compressed save + default repl-diskless-load (disabled): replica
    #    copies the payload to a temp file, then rdbLoad() auto-decompresses.
    test {Disk-receive (repl-diskless-load disabled) + diskless compressed save loads correctly} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary "diskrecv"

        start_server {overrides {save "" rdbcompression lz4 repl-diskless-load disabled}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "(disk-receive default load)"

            wait_for_log_messages -1 {"*target: replicas sockets*"} $primary_loglines 50 100
            wait_for_log_messages -1 {"*Diskless full sync with compression: lz4*"} $primary_loglines 50 100
            # Path B: load log reads "from <filename>", not "from primary".
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100

            $replica replicaof no one
        }
    }

    # 7. Diskless full-sync RDB payload compression: a diskless full sync compresses
    #    the RDB payload as an LZ4 VCS frame over the socket when the primary has
    #    rdbcompression lz4 AND the attaching replica advertised the
    #    capability. The $EOF:<mark> framing stays uncompressed so diskless transfer
    #    boundaries are unchanged.
    test {Diskless full sync compresses the RDB payload for a capable replica} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary diskless-full-sync

        start_server {overrides {save "" rdbcompression lz4 repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "Replica digest mismatch after compressed diskless full sync"

            wait_for_log_messages -1 {"*target: replicas sockets*"} $primary_loglines 50 100
            wait_for_log_messages -1 {"*Diskless full sync with compression: lz4*"} $primary_loglines 50 100
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4) from primary*"} $replica_loglines 50 100

            # The post-sync incremental stream still flows correctly.
            $primary set diskless-full-sync:post after
            wait_for_condition 50 100 {
                [$replica get diskless-full-sync:post] eq "after"
            } else {
                fail "Replica did not receive post-sync write"
            }

            $replica replicaof no one
        }
    }

}

# 3. Checksum interaction: a compressed diskless full sync loads and matches
#    under both rdbchecksum yes and no. (rdbchecksum is immutable, set via
#    startup override; the TLS-gated checksum-skip negotiation is not exercised.)
#    Isolated because rdbchecksum is an immutable startup-only override.

start_server {overrides {save "" rdbcompression lz4 repl-diskless-sync yes repl-diskless-sync-delay 0 rdbchecksum no}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Compressed diskless full sync loads with rdbchecksum no on the primary} {
        # Confirm the immutable override actually took effect.
        assert_equal "no" [lindex [$primary config get rdbchecksum] 1]
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary "cksum-off"

        start_server {overrides {save "" rdbcompression lz4 repl-diskless-load disabled}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "(checksum off)"

            wait_for_log_messages -1 {"*Diskless full sync with compression: lz4*"} $primary_loglines 50 100
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4)*"} $replica_loglines 50 100

            $replica replicaof no one
        }
    }
}

# 6. Mid-transfer link drop on a compressed diskless full sync: the replica's
#    inline socket decompressor (repl-diskless-load swapdb -> the
#    replicaLoadPrimaryRDBFromSocket TRUNCATED path) sees a clean EOF before the
#    LZ4 frame end. That is recoverable truncation, NOT codec corruption, so the
#    replica must survive, retry, and complete a clean resync rather than abort.
#    This is the integration counterpart of the unit test
#    streamReaderRejectsTruncatedFrameTrailer (clean EOF mid-frame -> TRUNCATED).
#
#    Determinism: compression makes the diskless child finish almost instantly,
#    so we stretch it with a per-key save delay over a sizable compressible
#    dataset to open an interrupt window. We watch the replica's log for the
#    "Loading compressed RDB" line rather than polling INFO because the replica
#    is blocked in the synchronous load and cannot answer INFO mid-load.

start_server {overrides {save "" rdbcompression lz4 repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Compressed diskless full sync recovers from a mid-transfer link drop (truncation, not corruption)} {
        # Sizable compressible dataset so the stretched diskless child streams the
        # compressed RDB slowly enough to interrupt mid-frame.
        fsc_populate $primary "trunc" 2000
        set sync_full_before [status $primary sync_full]

        start_server {overrides {save "" rdbcompression lz4 repl-diskless-load swapdb}} {
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
            # fsc_assert_synced waits up to 30s for the link to come back up
            # (reconnect is driven by the replication cron, ~1s) before the digest check.
            fsc_assert_synced $primary $replica "(truncation recovery)"

            # The interrupted attempt and the retry each count one full sync, so
            # this also proves the kill landed mid-transfer.
            assert {[status $primary sync_full] >= $sync_full_before + 2}

            # The interrupted transfer exercised the inline compressed-decode path
            # and was handled as a recoverable load failure -- not a crash or a
            # corruption abort. (`ping` above already proves the server is alive;
            # this also confirms there was no panic/assertion.)
            assert {[count_log_message 0 "Loading compressed RDB"] >= 1}
            assert {[count_log_message 0 "Failed trying to load the PRIMARY synchronization DB"] >= 1}
            assert_equal 0 [count_log_message 0 "=== ASSERTION FAILED ==="]
            # The recovery resync logged a clean success.
            assert {[count_log_message 0 "REPLICA sync: Finished with success"] >= 1}

            # Post-recovery incremental writes still flow.
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

# A mixed diskless cohort (one capable, one not) falls back to a plaintext RDB
# payload for all replicas: the group-AND of capabilities clears the compress-sync
# bit, so a single plaintext round serves everyone. A slow manual BGSAVE occupies
# the RDB child slot so both replicas park in WAIT_BGSAVE_START together and the
# grouped AND decision is exercised deterministically.
start_server {overrides {save "" rdbcompression lz4 repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    # Enough keys + per-key delay so the manual BGSAVE child stays alive long
    # enough for both diskless replicas to register.
    fsc_populate $primary mixed-diskless 800

    start_server {overrides {save "" rdbcompression lz4 repl-diskless-load swapdb}} {
        set replica_capable [srv 0 client]

        start_server {overrides {save "" rdbcompression yes repl-diskless-load swapdb}} {
            set replica_plain [srv 0 client]

            test {Mixed diskless cohort falls back to a plaintext RDB payload} {
                set rounds_before [count_log_message -2 {Starting BGSAVE for SYNC}]
                set primary_loglines [count_log_lines -2]

                # Occupy the RDB child slot with a slow manual BGSAVE so both
                # diskless replicas park in WAIT_BGSAVE_START together.
                $primary config set rdb-key-save-delay 5000
                assert_match {*Background saving started*} [$primary bgsave]
                wait_for_condition 200 10 {
                    [status $primary rdb_bgsave_in_progress] eq 1
                } else {
                    $primary config set rdb-key-save-delay 0
                    fail "manual BGSAVE did not start"
                }

                $replica_capable replicaof $primary_host $primary_port
                $replica_plain replicaof $primary_host $primary_port
                wait_for_condition 200 10 {
                    [status $primary connected_slaves] == 2 &&
                    [status $primary rdb_bgsave_in_progress] eq 1
                } else {
                    $primary config set rdb-key-save-delay 0
                    fail "both diskless replicas did not register before manual BGSAVE finished"
                }

                # Let the diskless round run fast once the manual child clears.
                $primary config set rdb-key-save-delay 0

                fsc_assert_synced $primary $replica_capable \
                    "Capable replica digest mismatch after mixed diskless full sync"
                fsc_assert_synced $primary $replica_plain \
                    "Plain replica digest mismatch after mixed diskless full sync"

                # A single grouped diskless round served both replicas.
                assert_equal 1 [expr {[count_log_message -2 {Starting BGSAVE for SYNC}] - $rounds_before}]

                # AND-rule result: the round is plaintext, so no compression NOTICE.
                verify_no_log_message -2 "*Diskless full sync with compression: lz4*" $primary_loglines

                $replica_plain replicaof no one
                $replica_capable replicaof no one
            }
        }
    }
}

# 10 + 11. Fake-primary negative coverage for the size-framed compressed socket
#     load. A complete VCS/LZ4 stream is captured from a throwaway server's
#     dump.rdb (rdbcompression lz4 makes SAVE write a full VCS stream), then
#     replayed by tests/helpers/fake_primary.tcl with a controlled bulk header
#     and payload. The fake primary is plain TCP, so skip under TLS.

if {!$::tls} {

# Capture a complete compressed stream once for both fake-primary tests.
set fsc_payload ""
start_server {overrides {save "" rdbcompression lz4}} {
    set src [srv 0 client]
    for {set i 0} {$i < 32} {incr i} {
        $src set src:key:$i [string repeat "srcval:$i " 8]
    }
    $src save
    set src_dump [fsc_dump_path $src]
    set fsc_payload [read_binary_file $src_dump]
}

# Fake-primary survivor cases (10 + 12 + 14) share ONE replica: each is a
# recoverable outcome (rejected/overrun load or truncation), so the replica
# process lives through all three. Each resets its own state (replicaof no one +
# config resetstat) to stay independent.

start_server {overrides {save "" rdbcompression lz4 repl-diskless-load swapdb}} {
    set replica [srv 0 client]

    # 10. Announced size larger than what is sent, then close. The complete frame
    #     decodes cleanly, so before the consumed-equals-announced check an early
    #     close looked like a successful load.
    test {Compressed socket load fails when the stream ends before the announced size} {
        assert_equal "VCS" [string range $fsc_payload 0 2]
        set replica_loglines [count_log_lines 0]
        set announce [expr {[string length $fsc_payload] + 100}]
        lassign [fsc_start_fake_primary $fsc_payload $announce] fake_pid fake_port

        $replica replicaof 127.0.0.1 $fake_port
        wait_for_log_messages 0 {"*ended before the announced transfer size*"} $replica_loglines 100 100

        # The replica survived the rejected load and kept nothing from it.
        assert_equal {PONG} [$replica ping]
        assert_equal 0 [$replica dbsize]

        $replica replicaof no one
        catch {exec kill $fake_pid}
    }

    # 12. Complete valid frame with the exact announced size, so the load
    #     succeeds and replication input accounting must count every encoded wire byte:
    #     total_net_repl_input_bytes == payload bytes N + the "$<N>\r\n" bulk
    #     header. Tight enough to catch a lost footer unlike the comp < plain/2
    #     check.
    test {Compressed socket load counts exactly the encoded wire bytes on a successful load} {
        assert_equal "VCS" [string range $fsc_payload 0 2]
        set n [string length $fsc_payload]
        $replica config resetstat
        lassign [fsc_start_fake_primary $fsc_payload $n] fake_pid fake_port

        $replica replicaof 127.0.0.1 $fake_port
        # The complete frame with the exact announced size loads cleanly.
        wait_for_condition 100 100 {
            [$replica dbsize] > 0
        } else {
            fail "replica did not load the compressed payload"
        }
        # Stop before the dropped link (fake primary closed) can reconnect.
        $replica replicaof no one

        # Bulk header "$<N>\r\n" = 1 ('$') + digits(N) + 2 ("\r\n").
        set meta [expr {3 + [string length $n]}]
        set expected [expr {$n + $meta}]
        set got [status $replica total_net_repl_input_bytes]
        assert_equal $expected $got \
            "net-input $got != N $n + bulk header $meta (expected $expected)"
        catch {exec kill $fake_pid}
    }

    # 14. Announce N but send a truncated prefix and close, so the load fails
    #     before the frame end and the reader's residual wire bytes must be
    #     counted before the failing path returns. Truncation is recoverable (not the
    #     corruption exit), so the replica survives and total_net_repl_input_bytes
    #     must equal the bytes actually sent plus the "$<N>\r\n" bulk header.
    test {Compressed socket load counts every wire byte received on a failing (truncated) transfer} {
        assert_equal "VCS" [string range $fsc_payload 0 2]
        set full_len [string length $fsc_payload]
        set bytes_sent [expr {$full_len / 2}]
        set truncated [string range $fsc_payload 0 [expr {$bytes_sent - 1}]]
        set announce $full_len

        $replica config resetstat
        set replica_loglines [count_log_lines 0]
        lassign [fsc_start_fake_primary $truncated $announce] fake_pid fake_port

        $replica replicaof 127.0.0.1 $fake_port
        wait_for_log_messages 0 {"*Failed trying to load the PRIMARY synchronization DB*"} $replica_loglines 100 100

        # Stop retrying against the now-dead fake primary before reading stats;
        # refused reconnects add no bytes, but this keeps the window clean.
        $replica replicaof no one
        assert_equal {PONG} [$replica ping]

        # "$<N>\r\n" bulk header = 1 ('$') + digits(N) + 2 ("\r\n").
        set meta [expr {3 + [string length $announce]}]
        set expected [expr {$bytes_sent + $meta}]
        set got [status $replica total_net_repl_input_bytes]
        assert_equal $expected $got \
            "net-input $got != bytes_sent $bytes_sent + bulk header $meta (expected $expected)"
        catch {exec kill $fake_pid}
    }
}

# 11 + 13. Corrupt compressed payloads: a flipped byte mid-frame (caught during
#     parse) and a flipped last byte (content-checksum mismatch caught at frame
#     finish) converge on the same fatal path. A corrupt diskless load is
#     terminal by design (rdbReportError logs and exits), so the replica process
#     dies each iteration and every variant runs in its own fresh replica.
test {Corrupted compressed stream (mid-frame and footer) aborts the diskless load} {
    foreach {label offset_expr} {
        mid-frame {[string length $fsc_payload] / 2}
        footer    {[string length $fsc_payload] - 1}
    } {
        start_server {overrides {save "" rdbcompression lz4 repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            set corrupt_at [expr $offset_expr]
            assert {$corrupt_at > 8} ;# flip a byte well past the 8-byte VCS envelope
            binary scan [string index $fsc_payload $corrupt_at] c byte_val
            set corrupted [string replace $fsc_payload $corrupt_at $corrupt_at \
                               [binary format c [expr {$byte_val ^ 0xff}]]]
            lassign [fsc_start_fake_primary $corrupted [string length $corrupted]] fake_pid fake_port

            $replica replicaof 127.0.0.1 $fake_port

            wait_for_condition 100 100 {
                ![is_alive [srv 0 pid]]
            } else {
                fail "replica did not exit on a corrupt compressed stream ($label)"
            }
            set stdout [srv 0 stdout]
            assert_equal 1 [count_message_lines $stdout "Corrupt streaming-compressed RDB input"]
            assert_equal 1 [count_message_lines $stdout "Terminating server after rdb file reading failure."]
            catch {exec kill $fake_pid}
        }
    }
}

} ;# end !tls

} ;# end tags
