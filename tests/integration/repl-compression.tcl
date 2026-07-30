tags {"repl repl-compression external:skip"} {

# ============================================================
# Config CRUD — single-server tests, no replication needed
# ============================================================

start_server {overrides {save "" repl-compression no}} {

    test {Repl compression config defaults are correct} {
        assert_equal "no" [lindex [r config get repl-compression] 1]
    }

    test {repl-compression can be toggled on and off} {
        r config set repl-compression lz4-stream
        assert_equal "lz4-stream" [lindex [r config get repl-compression] 1]
        r config set repl-compression no
        assert_equal "no" [lindex [r config get repl-compression] 1]
    }

    test {Repl compression configs survive CONFIG REWRITE and restart} {
        r config set repl-compression lz4-stream
        r config rewrite

        restart_server 0 true false

        assert_equal "lz4-stream" [lindex [r config get repl-compression] 1]

        # Restore default
        r config set repl-compression no
    }
}

# ============================================================
# Replication handshake behavior — primary + replica tests
# ============================================================

start_server {tags {"repl"} overrides {save ""}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Replica with repl-compression no does NOT send capa compression} {
        start_server {overrides {save "" repl-compression no}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # Full sync completes normally without compression capability
            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Replica with repl-compression lz4-stream and diskless load sends capa compression} {
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            set info [$primary info replication]
            assert_match "*slave0:*" $info
            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Replica with repl-compression lz4-stream and disk-backed load also negotiates compression} {
        $primary config set repl-compression lz4-stream
        set _code [catch {
            start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load disabled}} {
                set replica [srv 0 client]
                $replica replicaof $primary_host $primary_port

                wait_for_condition 50 100 {
                    [s 0 master_link_status] eq {up}
                } else {
                    fail "Replication not started"
                }

                # Disk-backed full sync completes, then compression activates for the
                # post-sync incremental stream (the full-sync RDB itself is never
                # compressed by this capability).
                wait_for_condition 50 100 {
                    [regexp -all "compression=lz4" [$primary info replication]] >= 1
                } else {
                    fail "Compression not negotiated for disk-backed replica"
                }

                # Exercise the compressed incremental stream over a disk-backed link.
                for {set i 0} {$i < 100} {incr i} {
                    $primary set "diskbacked:$i" [string repeat "v" 50]
                }
                wait_for_condition 50 100 {
                    [$replica get "diskbacked:99"] eq [string repeat "v" 50]
                } else {
                    fail "Disk-backed replica did not receive compressed incremental stream"
                }
                assert_equal [$primary debug digest] [$replica debug digest]

                $replica replicaof no one
            }
        } _res _opts]
        $primary config set repl-compression no
        return -options $_opts $_res
    }

    test {Primary receiving capa compression still completes full sync correctly (no-op)} {
        $primary flushall
        for {set i 0} {$i < 100} {incr i} {
            $primary set "noop:$i" [string repeat "value$i " 10]
        }

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up"
            } else {
                fail "Replica did not complete full sync"
            }

            # Data integrity check — primary records capa but takes no action
            assert_equal [string repeat "value42 " 10] [$replica get noop:42]
            assert_equal 100 [$replica dbsize]

            $replica replicaof no one
        }
    }

    test {Backward compatibility - older replica without capa compression connects successfully} {
        start_server {overrides {save ""}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Toggling repl-compression mid-runtime affects the next handshake} {
        # First sync with compression disabled
        start_server {overrides {save "" repl-compression no repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started with repl-compression no"
            }

            assert_equal {up} [s 0 master_link_status]

            # Toggle compression on at runtime
            $replica config set repl-compression lz4-stream

            # Disconnect and reconnect to trigger a new handshake
            $replica replicaof no one
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started after toggling repl-compression lz4-stream"
            }

            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Compressed incremental replication delivers correct data} {
        $primary config set repl-compression lz4-stream
        $primary flushall

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # Write data on primary AFTER full sync (goes through incremental stream)
            for {set i 0} {$i < 50} {incr i} {
                $primary set "compressed:$i" [string repeat "payload$i " 20]
            }

            # Wait for replica to catch up
            wait_for_condition 50 100 {
                [$replica dbsize] == [$primary dbsize]
            } else {
                fail "Replica did not catch up: replica=[$replica dbsize] primary=[$primary dbsize]"
            }

            # Verify data integrity
            for {set i 0} {$i < 50} {incr i} {
                assert_equal [string repeat "payload$i " 20] [$replica get "compressed:$i"]
            }

            $replica replicaof no one
        }

        $primary config set repl-compression no
    }

    test {Compressed incremental replication handles values larger than the batch limit} {
        # A value past REPL_COMPRESSION_BATCH_LIMIT (1 MB) is compressed across
        # multiple dispatches; verify it round-trips intact.
        $primary config set repl-compression lz4-stream
        $primary flushall

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # ~4 MB value (well past the 1 MB batch limit) written after full sync.
            set bigval [string repeat "abcdefghij0123456789" 209715]
            $primary set bigkey $bigval

            wait_for_condition 50 200 {
                [$replica get bigkey] eq $bigval
            } else {
                fail "Large value did not replicate intact under compression"
            }
            assert_equal [string length $bigval] [string length [$replica get bigkey]]

            $replica replicaof no one
        }

        $primary config set repl-compression no
    }

    test {Compressed incremental replication handles incompressible values larger than the batch limit} {
        # A pseudo-random (incompressible) value past REPL_COMPRESSION_BATCH_LIMIT
        # (1 MB) exercises the codec's ratio~1 expansion path across multiple
        # dispatches; verify it round-trips intact with no compression errors.
        $primary config set repl-compression lz4-stream
        $primary flushall

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # Deterministic pseudo-random binary payload >= 1.5 MiB, built in
            # 4096-byte chunks from a seeded PRNG so failures are reproducible.
            expr {srand(424242)}
            set payload ""
            while {[string length $payload] < 1572864} {
                set chunk ""
                for {set i 0} {$i < 4096} {incr i} {
                    append chunk [format %c [expr {int(rand()*256)}]]
                }
                append payload $chunk
            }
            $primary set incompressible_key $payload

            wait_for_condition 50 200 {
                [$replica get incompressible_key] eq $payload
            } else {
                fail "Incompressible value did not replicate intact under compression"
            }
            assert_equal {up} [s 0 master_link_status]

            set info [$primary info replication]
            assert_match "*compression=lz4*" $info
            assert {[regexp {compression_errors=([0-9]+)} $info -> comp_errors]}
            assert_equal 0 $comp_errors

            $replica replicaof no one
        }

        $primary config set repl-compression no
    }

    test {Partial resync with compression delivers correct data} {
        $primary config set repl-compression lz4-stream
        $primary flushall

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # Write initial data
            $primary set key1 value1

            wait_for_condition 50 100 {
                [$replica get key1] eq {value1}
            } else {
                fail "Initial replication failed"
            }

            # Break the replication link from the primary side. The replica keeps
            # its cached primary + offset and auto-reconnects, which exercises the
            # compressed *partial* resync path (REPLICAOF NO ONE would force a full
            # resync instead).
            set partial_before [status $primary sync_partial_ok]
            $primary client kill type replica

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Reconnection failed"
            }

            # Confirm a partial resync actually happened (not a silent full sync)
            wait_for_condition 50 100 {
                [status $primary sync_partial_ok] > $partial_before
            } else {
                fail "Expected a partial resync after reconnect, but none occurred"
            }

            # Write more data after partial resync
            $primary set key2 value2
            $primary set key3 [string repeat "x" 1000]

            wait_for_condition 50 100 {
                [$replica get key3] eq [string repeat "x" 1000]
            } else {
                fail "Post-partial-resync replication failed"
            }

            assert_equal value1 [$replica get key1]
            assert_equal value2 [$replica get key2]

            $replica replicaof no one
        }

        $primary config set repl-compression no
    }

    test {Replica with repl-compression lz4-stream handles a plaintext primary (passthrough)} {
        # Primary has compression OFF, replica ON: the replica advertises the
        # capability but the primary sends plaintext, so the replica must pass
        # the stream through untouched rather than expecting a VCS envelope.
        $primary config set repl-compression no
        $primary flushall

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started (primary plaintext, replica compression on)"
            }

            # Incremental writes arrive as plaintext; passthrough must deliver them.
            for {set i 0} {$i < 50} {incr i} {
                $primary set "pt:$i" [string repeat "payload$i " 20]
            }
            wait_for_condition 50 100 {
                [$replica get pt:49] eq [string repeat "payload49 " 20]
            } else {
                fail "Replica did not receive plaintext data via passthrough"
            }
            assert_equal [$primary dbsize] [$replica dbsize]

            # Primary never compressed (its config is off).
            assert_equal 0 [string match {*compression=lz4*} [$primary info replication]]

            $replica replicaof no one
        }
    }

    test {Replica repl-compression flip renegotiates upstream without manual reconnect} {
        $primary config set repl-compression lz4-stream

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 200 {
                [s 0 master_link_status] eq {up} &&
                [string match {*compression=lz4*} [$primary info replication]]
            } else {
                fail "Compressed replication not established"
            }

            # Flipping compression OFF on the replica must, on its own, drop and
            # reconnect the upstream link so it re-advertises capa without
            # compression. No manual replicaof is issued.
            $replica config set repl-compression no

            wait_for_condition 50 200 {
                [s 0 master_link_status] eq {up} &&
                ![string match {*compression=lz4*} [$primary info replication]]
            } else {
                fail "Replica did not renegotiate to plaintext after flip"
            }

            # Data still flows after the renegotiation.
            $primary set flipkey flipval
            wait_for_condition 50 100 {
                [$replica get flipkey] eq {flipval}
            } else {
                fail "Data not replicated after replica-side flip"
            }

            $replica replicaof no one
        }
    }

    test {CONFIG SET repl-compression no disconnects compressed replicas} {
        $primary config set repl-compression lz4-stream

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # Wait for compression to be active (replica must be state=online)
            wait_for_condition 50 200 {
                [string match {*state=online*compression=lz4*} [$primary info replication]]
            } else {
                fail "Compression not active on replica"
            }

            # Disable compression on primary — should disconnect compressed replicas
            $primary config set repl-compression no

            # Replica should disconnect and reconnect
            wait_for_condition 50 200 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replica did not reconnect after repl-compression disabled"
            }

            # After reconnect, compression should NOT be active
            set info [$primary info replication]
            if {[string match "*compression=lz4*" $info]} {
                fail "Compression still active after disable"
            }

            $replica replicaof no one
        }
    }

    test {Multiple compressed replicas receive replication correctly} {
        # Verifies that multiple compressed replicas can connect to the same
        # primary and all receive replication data. With IO threads enabled,
        # writes are distributed across the shared inbox.
        $primary config set repl-compression lz4-stream

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica1 [srv 0 client]
            $replica1 replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replica 1 not started"
            }

            start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
                set replica2 [srv 0 client]
                $replica2 replicaof $primary_host $primary_port

                wait_for_condition 50 100 {
                    [s 0 master_link_status] eq {up}
                } else {
                    fail "Replica 2 not started"
                }

                # Write data and verify both replicas receive it
                for {set i 0} {$i < 30} {incr i} {
                    $primary set "multi_repl:$i" "value_$i"
                }

                wait_for_condition 50 100 {
                    [$replica1 get "multi_repl:29"] eq {value_29} &&
                    [$replica2 get "multi_repl:29"] eq {value_29}
                } else {
                    fail "Not all replicas caught up"
                }

                # Verify both have compression active
                set info [$primary info replication]
                set matches [regexp -all "compression=lz4" $info]
                assert {$matches >= 2}

                $replica2 replicaof no one
            }
            $replica1 replicaof no one
        }
        $primary config set repl-compression no
    }

    # ============================================================
    # Comprehensive data-type coverage with compression enabled
    # ============================================================

    if {[lsearch $::denytags "repl-compression-suite"] == -1} {

    $primary config set repl-compression lz4-stream
    $primary flushall

    start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
        set replica [srv 0 client]
        $replica replicaof $primary_host $primary_port

        wait_for_condition 50 100 {
            [s 0 master_link_status] eq {up}
        } else {
            fail "Replication not started"
        }

        # Wait for replica to be fully online with compression active
        wait_for_condition 50 200 {
            [string match {*state=online*compression=lz4*} [$primary info replication]]
        } else {
            fail "Compression not active on replica"
        }

        test {Compressed replication handles strings correctly} {
            $primary set str_key [string repeat "hello world " 100]
            wait_for_condition 50 100 {
                [$replica get str_key] eq [string repeat "hello world " 100]
            } else {
                fail "String replication failed"
            }
        }

        test {Compressed replication handles lists correctly} {
            for {set i 0} {$i < 100} {incr i} {
                $primary rpush mylist "element_$i"
            }
            wait_for_condition 50 100 {
                [$replica llen mylist] == 100
            } else {
                fail "List replication failed: got [$replica llen mylist]"
            }
            assert_equal "element_0" [$replica lindex mylist 0]
            assert_equal "element_99" [$replica lindex mylist 99]
        }

        test {Compressed replication handles hashes correctly} {
            for {set i 0} {$i < 50} {incr i} {
                $primary hset myhash field_$i [string repeat "value$i" 20]
            }
            wait_for_condition 50 100 {
                [$replica hlen myhash] == 50
            } else {
                fail "Hash replication failed"
            }
            assert_equal [string repeat "value25" 20] [$replica hget myhash field_25]
        }

        test {Compressed replication handles sets correctly} {
            for {set i 0} {$i < 100} {incr i} {
                $primary sadd myset "member_$i"
            }
            wait_for_condition 50 100 {
                [$replica scard myset] == 100
            } else {
                fail "Set replication failed"
            }
            assert_equal 1 [$replica sismember myset "member_50"]
        }

        test {Compressed replication handles sorted sets correctly} {
            for {set i 0} {$i < 100} {incr i} {
                $primary zadd myzset $i "member_$i"
            }
            wait_for_condition 50 100 {
                [$replica zcard myzset] == 100
            } else {
                fail "Sorted set replication failed"
            }
            assert_equal "member_0" [lindex [$replica zrange myzset 0 0] 0]
        }

        test {Compressed replication handles large pipeline correctly} {
            for {set i 0} {$i < 1000} {incr i} {
                $primary set "pipeline:$i" [string repeat "x" 100]
            }
            wait_for_condition 50 200 {
                [$replica get "pipeline:999"] eq [string repeat "x" 100]
            } else {
                fail "Pipeline replication failed"
            }
            assert_equal [string repeat "x" 100] [$replica get "pipeline:500"]
        }

        test {Compressed replication handles MULTI/EXEC correctly} {
            $primary multi
            $primary set tx_key1 tx_val1
            $primary set tx_key2 tx_val2
            $primary incr tx_counter
            $primary exec
            wait_for_condition 50 100 {
                [$replica get tx_key2] eq {tx_val2}
            } else {
                fail "Transaction replication failed"
            }
            assert_equal "1" [$replica get tx_counter]
        }

        test {Compressed replication handles DEL and expiry correctly} {
            $primary set expire_key "will_expire"
            $primary pexpire expire_key 100
            $primary set del_key "will_delete"
            $primary del del_key
            after 200
            wait_for_condition 50 100 {
                [$replica exists expire_key] == 0
            } else {
                fail "Expiry replication failed"
            }
            assert_equal 0 [$replica exists del_key]
        }

        $replica replicaof no one
    }

    test {Compression handles io-threads change gracefully} {
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # Write some data
            $primary set io_test_key "io_test_value"
            wait_for_condition 50 100 {
                [$replica get io_test_key] eq {io_test_value}
            } else {
                fail "Initial replication failed"
            }

            # Verify data still replicates after more writes
            for {set i 0} {$i < 20} {incr i} {
                $primary set "after_io_change:$i" "value_$i"
            }
            wait_for_condition 50 100 {
                [$replica get "after_io_change:19"] eq {value_19}
            } else {
                fail "Replication after io-threads context failed"
            }

            $replica replicaof no one
        }
    }

    $primary config set repl-compression no

    } ;# end repl-compression-suite
}

# ============================================================
# Multi-replica compressed replication tests
# ============================================================

# Disabling repl-compression at runtime disconnects compressed replicas. The
# disconnect is deferred until after CONFIG SET commits (so a rolled-back
# multi-option CONFIG SET drops nothing), then the replica reconnects plaintext.
start_server {tags {"repl"} overrides {save "" repl-compression lz4-stream}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Disabling repl-compression disconnects compressed replicas} {
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [regexp -all "compression=lz4" [$primary info replication]] >= 1
            } else {
                fail "Compression not active before disable"
            }

            $primary config set repl-compression no

            # The compressed replica is dropped, then reconnects as plaintext, so
            # the link no longer reports compression=lz4.
            wait_for_condition 50 100 {
                [regexp -all "compression=lz4" [$primary info replication]] == 0
            } else {
                fail "Compressed replica was not disconnected after repl-compression no"
            }
            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up"
            } else {
                fail "Replica did not reconnect after compression disabled"
            }

            $replica replicaof no one
        }
        $primary config set repl-compression lz4-stream
    }
}

# Enabling repl-compression at runtime disconnects capable-but-plaintext replicas
# so they reconnect compressed (symmetric with the disable case; deferred to
# CONFIG SET commit so a rolled-back command reconnects nothing).
start_server {tags {"repl"} overrides {save "" repl-compression no}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Enabling repl-compression reconnects capable replicas compressed} {
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            # Primary compression is off, so although the replica advertised the
            # capability the negotiated link is plaintext.
            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up"
            } else {
                fail "Replica did not sync"
            }
            assert_equal 0 [regexp -all "compression=lz4" [$primary info replication]]

            # Enable on the primary: the capable replica is dropped and reconnects
            # over a compressed stream.
            $primary config set repl-compression lz4-stream
            wait_for_condition 50 100 {
                [regexp -all "compression=lz4" [$primary info replication]] >= 1
            } else {
                fail "Capable replica did not reconnect compressed after enable"
            }

            # Data flows correctly over the new compressed link.
            $primary set enabled_key enabled_val
            wait_for_condition 50 100 {
                [$replica get enabled_key] eq "enabled_val"
            } else {
                fail "Compressed replication did not deliver after enable"
            }
            assert_equal [$primary debug digest] [$replica debug digest]

            $replica replicaof no one
        }
    }
}

# Test 4: Multiple replicas distribute across threads and stay in sync
start_server {tags {"repl"} overrides {save "" io-threads 4 io-threads-always-active yes repl-compression lz4-stream}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Multiple replicas all stay in sync under load} {
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica1 [srv 0 client]
            $replica1 replicaof $primary_host $primary_port
            wait_for_sync $replica1

            start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
                set replica2 [srv 0 client]
                $replica2 replicaof $primary_host $primary_port
                wait_for_sync $replica2

                start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
                    set replica3 [srv 0 client]
                    $replica3 replicaof $primary_host $primary_port
                    wait_for_sync $replica3

                    # Wait for all replicas to have compression active
                    wait_for_condition 50 200 {
                        [regexp -all "compression=lz4" [$primary info replication]] >= 3
                    } else {
                        fail "Not all replicas have compression active"
                    }

                    # Generate sustained load
                    for {set i 0} {$i < 500} {incr i} {
                        $primary set "multi_repl:$i" [string repeat "x" 100]
                    }

                    # Wait for all replicas to catch up
                    wait_for_condition 100 200 {
                        [$replica1 dbsize] == [$primary dbsize] &&
                        [$replica2 dbsize] == [$primary dbsize] &&
                        [$replica3 dbsize] == [$primary dbsize]
                    } else {
                        fail "Not all replicas caught up: r1=[$replica1 dbsize] r2=[$replica2 dbsize] r3=[$replica3 dbsize] primary=[$primary dbsize]"
                    }

                    # Verify data integrity
                    set primary_digest [$primary debug digest]
                    assert_equal $primary_digest [$replica1 debug digest]
                    assert_equal $primary_digest [$replica2 debug digest]
                    assert_equal $primary_digest [$replica3 debug digest]

                    $replica3 replicaof no one
                }
                $replica2 replicaof no one
            }
            $replica1 replicaof no one
        }
    }
}

# Test 5: Compressed replication survives replica disconnect/reconnect
start_server {tags {"repl"} overrides {save "" io-threads 4 io-threads-always-active yes repl-compression lz4-stream}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Compressed replication survives replica disconnect and reconnect} {
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica1 [srv 0 client]
            $replica1 replicaof $primary_host $primary_port
            wait_for_sync $replica1

            start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
                set replica2 [srv 0 client]
                $replica2 replicaof $primary_host $primary_port
                wait_for_sync $replica2

                # Drive traffic, verify both in sync
                for {set i 0} {$i < 200} {incr i} {
                    $primary set "pre_disconnect:$i" [string repeat "x" 50]
                }

                wait_for_condition 50 200 {
                    [$replica1 dbsize] == [$primary dbsize] &&
                    [$replica2 dbsize] == [$primary dbsize]
                } else {
                    fail "Replicas not in sync before disconnect"
                }

                # Disconnect replica1
                $replica1 replicaof no one

                # Drive more traffic — replica2 should stay connected and in sync
                for {set i 0} {$i < 200} {incr i} {
                    $primary set "post_disconnect:$i" [string repeat "x" 50]
                }

                wait_for_condition 50 200 {
                    [$replica2 get "post_disconnect:199"] eq [string repeat "x" 50]
                } else {
                    fail "Replica2 did not stay in sync after replica1 disconnect"
                }

                # Reconnect replica1
                $replica1 replicaof $primary_host $primary_port

                wait_for_condition 50 200 {
                    [status $replica1 master_link_status] eq "up"
                } else {
                    fail "Replica1 did not reconnect"
                }

                # Wait for replica1 to catch up
                wait_for_condition 50 200 {
                    [$replica1 dbsize] == [$primary dbsize]
                } else {
                    fail "Replica1 did not re-sync: replica1=[$replica1 dbsize] primary=[$primary dbsize]"
                }

                # Verify compression is re-negotiated on replica1
                wait_for_condition 50 200 {
                    [regexp -all "compression=lz4" [$primary info replication]] >= 2
                } else {
                    fail "Compression not re-negotiated after reconnect"
                }

                # Verify data integrity
                assert_equal [$primary debug digest] [$replica1 debug digest]
                assert_equal [$primary debug digest] [$replica2 debug digest]

                $replica2 replicaof no one
            }
            $replica1 replicaof no one
        }
    }
}

# Chained replication: each hop negotiates compression independently, and the
# middle node simultaneously decodes its primary link on the main thread while
# encoding for its own replica on IO threads.
start_server {tags {"repl"} overrides {save "" repl-compression lz4-stream}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Chained replication compresses each hop independently} {
        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb io-threads 4 io-threads-always-active yes}} {
            set middle [srv 0 client]
            set middle_host [srv 0 host]
            set middle_port [srv 0 port]

            start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
                set leaf [srv 0 client]

                $middle replicaof $primary_host $primary_port
                wait_for_sync $middle
                $leaf replicaof $middle_host $middle_port
                wait_for_sync $leaf

                # Both hops negotiated compression.
                wait_for_condition 50 200 {
                    [regexp -all "compression=lz4" [$primary info replication]] >= 1 &&
                    [regexp -all "compression=lz4" [$middle info replication]] >= 1
                } else {
                    fail "Compression not active on both hops"
                }

                # Writes flow primary -> middle -> leaf across two compressed hops.
                for {set i 0} {$i < 200} {incr i} {
                    $primary set "chain:$i" "chain_value_$i"
                }
                wait_for_condition 50 200 {
                    [$leaf dbsize] == [$primary dbsize]
                } else {
                    fail "Leaf did not catch up: leaf=[$leaf dbsize] primary=[$primary dbsize]"
                }
                assert_equal "chain_value_0" [$leaf get chain:0]
                assert_equal "chain_value_99" [$leaf get chain:99]
                assert_equal "chain_value_199" [$leaf get chain:199]

                # Flip compression off on the leaf only and force a clean
                # reconnect: hop2 renegotiates plaintext, hop1 stays compressed.
                $leaf config set repl-compression no
                $leaf replicaof no one
                $leaf replicaof $middle_host $middle_port
                wait_for_sync $leaf

                wait_for_condition 50 200 {
                    [regexp -all "compression=lz4" [$middle info replication]] == 0
                } else {
                    fail "Hop2 still compressed after leaf disabled repl-compression"
                }
                assert {[regexp -all "compression=lz4" [$primary info replication]] >= 1}

                # Data still flows end-to-end over mixed hops.
                $primary set chain:final final_val
                wait_for_condition 50 200 {
                    [$leaf get chain:final] eq {final_val}
                } else {
                    fail "Write did not reach leaf after hop2 renegotiated plaintext"
                }

                $leaf replicaof no one
            }
            $middle replicaof no one
        }
    }
}


# ============================================================
# Diskless full-sync RDB payload compression
# ============================================================

# A diskless full sync compresses the RDB payload as an LZ4 VCS frame over the
# socket when the primary has repl-compression lz4-stream AND the attaching
# replica advertised the compression capability (its own repl-compression is
# lz4-stream). The $EOF:<mark> framing stays uncompressed so diskless transfer
# boundaries are unchanged. We assert the primary's "with compression: lz4"
# BGSAVE log, the replica's "Loading compressed RDB" log, and a digest match.

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

start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Diskless full sync compresses the RDB payload for a capable replica} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary diskless-full-sync

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
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

    test {Diskless full sync compression works when the replica loads via disk first} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary diskless-disk-load

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load disabled}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica \
                "Replica digest mismatch after compressed diskless full sync loaded from disk"

            wait_for_log_messages -1 {"*target: replicas sockets*"} $primary_loglines 50 100
            wait_for_log_messages -1 {"*Diskless full sync with compression: lz4*"} $primary_loglines 50 100

            $replica replicaof no one
        }
    }
}

# Dual-channel diskless full sync must also compress the RDB payload, which
# exercises the capa-compression advertising added to the dual-channel
# handshake.
start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 0 dual-channel-replication-enabled yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Dual-channel diskless full sync compresses the RDB payload} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary dual-channel-diskless

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb dual-channel-replication-enabled yes}} {
            set replica [srv 0 client]
            set replica_loglines [count_log_lines 0]
            $replica replicaof $primary_host $primary_port

            fsc_assert_synced $primary $replica "Replica digest mismatch after compressed dual-channel full sync"

            wait_for_log_messages -1 {"*using: dual-channel*"} $primary_loglines 50 100
            wait_for_log_messages -1 {"*Diskless full sync with compression: lz4*"} $primary_loglines 50 100
            wait_for_log_messages 0 {"*Loading compressed RDB (algo=lz4) from primary*"} $replica_loglines 50 100

            $replica replicaof no one
        }
    }
}

# A mixed diskless cohort (one capable, one not) must fall back to a plaintext
# RDB payload for all replicas, since the group-AND of capabilities clears the
# compression capability.
start_server {overrides {save "" repl-compression lz4-stream repl-diskless-sync yes repl-diskless-sync-delay 1000 repl-diskless-sync-max-replicas 2}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Mixed diskless cohort falls back to a plaintext RDB payload} {
        set primary_loglines [count_log_lines 0]
        fsc_populate $primary mixed-diskless

        start_server {overrides {save "" repl-compression lz4-stream repl-diskless-load swapdb}} {
            set replica_capable [srv 0 client]
            set replica_capable_host [srv 0 host]

            start_server {overrides {save "" repl-compression no repl-diskless-load swapdb}} {
                set replica_plain [srv 0 client]

                $replica_capable replicaof $primary_host $primary_port
                $replica_plain replicaof $primary_host $primary_port

                fsc_assert_synced $primary $replica_capable \
                    "Capable replica digest mismatch after mixed diskless full sync"
                fsc_assert_synced $primary $replica_plain \
                    "Plain replica digest mismatch after mixed diskless full sync"

                verify_no_log_message -1 "*with compression: lz4*" $primary_loglines

                $replica_plain replicaof no one
            }
            $replica_capable replicaof no one
        }
    }
}

}
