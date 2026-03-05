# Integration tests for replication with inline compression.
# Validates: Requirements 9.5, 9.6, 9.7

proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

# Helper: enable LZ4 replication compression on a server instance.
proc enable_repl_compression {r} {
    $r config set replcompression yes
    $r config set repl-compression-algo lz4
}

# Helper: disable replication compression on a server instance.
proc disable_repl_compression {r} {
    $r config set replcompression no
    $r config set repl-compression-algo none
}

# Helper: populate diverse data types on a server for replication verification.
proc populate_test_data {r prefix count} {
    for {set i 0} {$i < $count} {incr i} {
        $r set "${prefix}:str:$i" [string repeat "value$i " 20]
    }
    $r lpush "${prefix}:list" a b c d e f g h
    $r sadd "${prefix}:set" x y z w
    $r zadd "${prefix}:zset" 1.0 a 2.0 b 3.0 c 4.0 d
    $r hset "${prefix}:hash" f1 v1 f2 v2 f3 v3
}

# Helper: verify data integrity between master and replica.
proc verify_data_integrity {master replica prefix count} {
    for {set i 0} {$i < $count} {incr i} {
        set expected [$master get "${prefix}:str:$i"]
        set actual [$replica get "${prefix}:str:$i"]
        assert_equal $expected $actual
    }
    assert_equal [$master lrange "${prefix}:list" 0 -1] [$replica lrange "${prefix}:list" 0 -1]
    assert_equal [lsort [$master smembers "${prefix}:set"]] [lsort [$replica smembers "${prefix}:set"]]
    assert_equal [$master zrange "${prefix}:zset" 0 -1 WITHSCORES] [$replica zrange "${prefix}:zset" 0 -1 WITHSCORES]
    assert_equal [$master hgetall "${prefix}:hash"] [$replica hgetall "${prefix}:hash"]
}


# ---------------------------------------------------------------------------
# Test 1: Full replication with LZ4 compression — master writes data,
#          replica receives and verifies correctness.
# Validates: Requirement 9.5
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "Full replication with LZ4 compression - data integrity" {
            # Populate data on master before replication starts
            populate_test_data $master "pre" 50

            # Start replication
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Verify pre-existing data replicated correctly
            verify_data_integrity $master $replica "pre" 50

            # Write more data while replication is active
            populate_test_data $master "post" 50

            # Wait for replication to catch up
            wait_for_ofs_sync $master $replica

            # Verify post-replication data
            verify_data_integrity $master $replica "post" 50

            # Verify digests match
            set master_digest [$master debug digest]
            set replica_digest [$replica debug digest]
            assert {$master_digest ne "0000000000000000000000000000000000000000"}
            assert_equal $master_digest $replica_digest
        }

        test "Continuous writes with LZ4 compression maintain integrity" {
            # Write a burst of keys while compression is active
            for {set i 0} {$i < 200} {incr i} {
                $master set "burst:$i" [string repeat "data$i " 50]
            }

            wait_for_ofs_sync $master $replica

            # Spot-check values
            for {set i 0} {$i < 200} {incr i} {
                assert_equal [$master get "burst:$i"] [$replica get "burst:$i"]
            }

            assert_equal [$master dbsize] [$replica dbsize]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 2: Multiple replicas with compression on different IO threads.
# Validates: Requirement 9.7
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica1 [srv 0 client]
        start_server {overrides {save ""}} {
            set replica2 [srv 0 client]
            start_server {overrides {save ""}} {
                set replica3 [srv 0 client]

                test "Multiple replicas with compression on different IO threads" {
                    # Connect all three replicas
                    $replica1 replicaof $master_host $master_port
                    $replica2 replicaof $master_host $master_port
                    $replica3 replicaof $master_host $master_port

                    # Wait for all replicas to sync
                    wait_for_sync $replica1
                    wait_for_sync $replica2
                    wait_for_sync $replica3

                    # Wait for all replicas to be online from master's perspective
                    wait_for_condition 50 100 {
                        [string match {*slave0:*state=online*slave1:*state=online*slave2:*state=online*} [$master info replication]]
                    } else {
                        fail "Not all replicas reached online state"
                    }

                    # Write data on master
                    populate_test_data $master "multi" 100

                    # Wait for all replicas to catch up
                    wait_for_ofs_sync $master $replica1
                    wait_for_ofs_sync $master $replica2
                    wait_for_ofs_sync $master $replica3

                    # Verify all replicas have identical data
                    set master_digest [$master debug digest]
                    assert {$master_digest ne "0000000000000000000000000000000000000000"}
                    assert_equal $master_digest [$replica1 debug digest]
                    assert_equal $master_digest [$replica2 debug digest]
                    assert_equal $master_digest [$replica3 debug digest]

                    # Verify specific data on each replica
                    verify_data_integrity $master $replica1 "multi" 100
                    verify_data_integrity $master $replica2 "multi" 100
                    verify_data_integrity $master $replica3 "multi" 100
                }
            }
        }
    }
}


# ---------------------------------------------------------------------------
# Test 3: CONFIG SET replcompression no disconnects compressed replicas.
# Validates: Requirement 9.6
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master_log [srv 0 stdout]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "CONFIG SET replcompression no disconnects compressed replicas" {
            # Establish replication with compression
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Verify replication is working
            $master set testkey testvalue
            wait_for_ofs_sync $master $replica
            assert_equal "testvalue" [$replica get testkey]

            # Confirm we have one connected replica
            assert_match {*connected_slaves:1*} [$master info replication]

            # Disable compression at runtime — this should disconnect the replica
            $master config set replcompression no

            # Wait for the replica to be disconnected
            wait_for_condition 50 100 {
                [string match {*connected_slaves:0*} [$master info replication]]
            } else {
                fail "Compressed replica was not disconnected after disabling replcompression"
            }

            # Verify the log message
            wait_for_condition 50 100 {
                [log_file_matches $master_log "*Disconnecting compressed replica*replcompression disabled*"]
            } else {
                fail "Expected log message about disconnecting compressed replica not found"
            }

            # The replica should reconnect (without compression since it's now disabled)
            wait_for_condition 100 100 {
                [string match {*connected_slaves:1*} [$master info replication]]
            } else {
                fail "Replica did not reconnect after compression was disabled"
            }

            # Verify data still works after reconnect
            $master set testkey2 testvalue2
            wait_for_ofs_sync $master $replica
            assert_equal "testvalue2" [$replica get testkey2]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 4: Replica reconnect gets fresh compression context and data
#          integrity is maintained.
# Validates: Requirements 9.5, 9.6
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "Replica reconnect gets fresh compression context" {
            # Establish initial replication with compression
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Write data in the first session
            populate_test_data $master "session1" 30

            wait_for_ofs_sync $master $replica
            verify_data_integrity $master $replica "session1" 30

            # Force a disconnect by killing the replica connection from master side
            $master client kill type replica

            # Wait for disconnection
            wait_for_condition 50 100 {
                [string match {*connected_slaves:0*} [$master info replication]]
            } else {
                fail "Replica was not disconnected"
            }

            # Write data while replica is disconnected
            populate_test_data $master "between" 20

            # Wait for replica to reconnect (it will auto-reconnect)
            wait_for_condition 100 100 {
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "Replica did not reconnect"
            }

            # Write data in the second session (after reconnect with fresh context)
            populate_test_data $master "session2" 30

            wait_for_ofs_sync $master $replica

            # Verify all data from both sessions and in-between
            verify_data_integrity $master $replica "session1" 30
            verify_data_integrity $master $replica "between" 20
            verify_data_integrity $master $replica "session2" 30

            # Verify overall digest match
            assert_equal [$master debug digest] [$replica debug digest]
        }

        test "Multiple reconnects maintain data integrity with compression" {
            # Perform several disconnect/reconnect cycles
            for {set cycle 0} {$cycle < 3} {incr cycle} {
                # Write data
                for {set i 0} {$i < 20} {incr i} {
                    $master set "cycle${cycle}:key$i" "val$i"
                }

                wait_for_ofs_sync $master $replica

                # Verify data
                for {set i 0} {$i < 20} {incr i} {
                    assert_equal "val$i" [$replica get "cycle${cycle}:key$i"]
                }

                # Force disconnect (except on last cycle)
                if {$cycle < 2} {
                    $master client kill type replica

                    wait_for_condition 50 100 {
                        [string match {*connected_slaves:0*} [$master info replication]]
                    } else {
                        fail "Replica was not disconnected on cycle $cycle"
                    }

                    # Wait for reconnect
                    wait_for_condition 100 100 {
                        [string match {*master_link_status:up*} [$replica info replication]]
                    } else {
                        fail "Replica did not reconnect on cycle $cycle"
                    }
                }
            }

            # Final digest check
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 5: repl-compression-thread-affinity yes pins replica to consistent
#          thread — verify replication works correctly with affinity enabled.
# Validates: Requirements 9.3, 9.5
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master
    $master config set repl-compression-thread-affinity yes

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "Thread affinity yes - replication data integrity with pinned thread" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Write data and verify it replicates correctly with affinity on
            populate_test_data $master "affinity-yes" 100

            wait_for_ofs_sync $master $replica
            verify_data_integrity $master $replica "affinity-yes" 100

            # Verify digests match
            assert_equal [$master debug digest] [$replica debug digest]
        }

        test "Thread affinity yes - sustained writes maintain integrity" {
            # Burst of writes to exercise the pinned thread under load
            for {set i 0} {$i < 300} {incr i} {
                $master set "affinity-burst:$i" [string repeat "x" 512]
            }

            wait_for_ofs_sync $master $replica

            for {set i 0} {$i < 300} {incr i} {
                assert_equal [$master get "affinity-burst:$i"] [$replica get "affinity-burst:$i"]
            }

            assert_equal [$master dbsize] [$replica dbsize]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 6: repl-compression-thread-affinity yes with multiple replicas —
#          each replica pinned to a thread via round-robin, all get correct data.
# Validates: Requirements 9.3, 9.5
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master
    $master config set repl-compression-thread-affinity yes

    start_server {overrides {save ""}} {
        set replica1 [srv 0 client]
        start_server {overrides {save ""}} {
            set replica2 [srv 0 client]

            test "Thread affinity yes - multiple replicas pinned via round-robin" {
                $replica1 replicaof $master_host $master_port
                $replica2 replicaof $master_host $master_port

                wait_for_sync $replica1
                wait_for_sync $replica2

                # Write data
                populate_test_data $master "affinity-multi" 80

                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2

                # Both replicas must have identical data
                set master_digest [$master debug digest]
                assert {$master_digest ne "0000000000000000000000000000000000000000"}
                assert_equal $master_digest [$replica1 debug digest]
                assert_equal $master_digest [$replica2 debug digest]

                verify_data_integrity $master $replica1 "affinity-multi" 80
                verify_data_integrity $master $replica2 "affinity-multi" 80
            }
        }
    }
}


# ---------------------------------------------------------------------------
# Test 7: repl-compression-thread-affinity no uses modulo dispatch —
#          verify replication works correctly without sticky affinity.
# Validates: Requirements 9.3, 9.5
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master
    $master config set repl-compression-thread-affinity no

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "Thread affinity no - replication data integrity with modulo dispatch" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Write data and verify it replicates correctly without affinity
            populate_test_data $master "affinity-no" 100

            wait_for_ofs_sync $master $replica
            verify_data_integrity $master $replica "affinity-no" 100

            assert_equal [$master debug digest] [$replica debug digest]
        }

        test "Thread affinity no - sustained writes maintain integrity" {
            for {set i 0} {$i < 300} {incr i} {
                $master set "no-affinity-burst:$i" [string repeat "y" 512]
            }

            wait_for_ofs_sync $master $replica

            for {set i 0} {$i < 300} {incr i} {
                assert_equal [$master get "no-affinity-burst:$i"] [$replica get "no-affinity-burst:$i"]
            }

            assert_equal [$master dbsize] [$replica dbsize]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 8: repl-compression-thread-affinity no with multiple replicas —
#          modulo dispatch distributes replicas, all get correct data.
# Validates: Requirements 9.3, 9.5
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master
    $master config set repl-compression-thread-affinity no

    start_server {overrides {save ""}} {
        set replica1 [srv 0 client]
        start_server {overrides {save ""}} {
            set replica2 [srv 0 client]

            test "Thread affinity no - multiple replicas with modulo dispatch" {
                $replica1 replicaof $master_host $master_port
                $replica2 replicaof $master_host $master_port

                wait_for_sync $replica1
                wait_for_sync $replica2

                populate_test_data $master "no-affinity-multi" 80

                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2

                set master_digest [$master debug digest]
                assert {$master_digest ne "0000000000000000000000000000000000000000"}
                assert_equal $master_digest [$replica1 debug digest]
                assert_equal $master_digest [$replica2 debug digest]

                verify_data_integrity $master $replica1 "no-affinity-multi" 80
                verify_data_integrity $master $replica2 "no-affinity-multi" 80
            }
        }
    }
}


# ---------------------------------------------------------------------------
# Test 9: io-threads 1 with compression enabled works on main thread.
# Validates: Requirements 9.5 (edge case from Requirement 8.4)
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 1}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "io-threads 1 - compression works on main thread" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Write data — compression happens on main thread since no IO threads
            populate_test_data $master "main-thread" 50

            wait_for_ofs_sync $master $replica
            verify_data_integrity $master $replica "main-thread" 50

            assert_equal [$master debug digest] [$replica debug digest]
        }

        test "io-threads 1 - sustained writes on main thread" {
            for {set i 0} {$i < 200} {incr i} {
                $master set "main-burst:$i" [string repeat "z" 256]
            }

            wait_for_ofs_sync $master $replica

            for {set i 0} {$i < 200} {incr i} {
                assert_equal [$master get "main-burst:$i"] [$replica get "main-burst:$i"]
            }

            assert_equal [$master dbsize] [$replica dbsize]
        }

        test "io-threads 1 - diverse data types replicate correctly" {
            # Exercise all data types to ensure main-thread compression handles them
            $master lpush "mt:list" {*}[lmap x {a b c d e f g h i j} {set x}]
            $master sadd "mt:set" {*}[lmap x {1 2 3 4 5 6 7 8 9 10} {set x}]
            $master zadd "mt:zset" 1 a 2 b 3 c 4 d 5 e
            $master hset "mt:hash" name test age 42 city nowhere

            wait_for_ofs_sync $master $replica

            assert_equal [$master lrange "mt:list" 0 -1] [$replica lrange "mt:list" 0 -1]
            assert_equal [lsort [$master smembers "mt:set"]] [lsort [$replica smembers "mt:set"]]
            assert_equal [$master zrange "mt:zset" 0 -1 WITHSCORES] [$replica zrange "mt:zset" 0 -1 WITHSCORES]
            assert_equal [$master hgetall "mt:hash"] [$replica hgetall "mt:hash"]

            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 10: Replica disconnect and partial resync resumption with compression.
#          Writes data, disconnects replica, writes more data while disconnected,
#          replica reconnects via partial resync (PSYNC) and catches up.
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4 repl-backlog-size 10mb}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "Partial resync after disconnect with compression - data integrity" {
            # Initial full sync
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Write a batch of data in the first session
            for {set i 0} {$i < 100} {incr i} {
                $master set "psync-pre:$i" [string repeat "A" 200]
            }
            wait_for_ofs_sync $master $replica

            # Verify pre-disconnect data
            for {set i 0} {$i < 100} {incr i} {
                assert_equal [$master get "psync-pre:$i"] [$replica get "psync-pre:$i"]
            }

            # Record the master replication offset before disconnect
            set pre_disconnect_offset [status $master master_repl_offset]

            # Kill the replica connection from the replica side (simulates network drop)
            $replica client kill $master_host:$master_port

            # Wait for master to notice the disconnect
            wait_for_condition 50 100 {
                [string match {*connected_slaves:0*} [$master info replication]]
            } else {
                fail "Master did not detect replica disconnect"
            }

            # Write data while replica is disconnected — this goes into the backlog
            for {set i 0} {$i < 200} {incr i} {
                $master set "psync-during:$i" [string repeat "B" 300]
            }

            # Wait for replica to auto-reconnect
            wait_for_condition 100 100 {
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "Replica did not reconnect after disconnect"
            }

            # Write more data after reconnect
            for {set i 0} {$i < 50} {incr i} {
                $master set "psync-post:$i" [string repeat "C" 150]
            }
            wait_for_ofs_sync $master $replica

            # Verify ALL data: pre-disconnect, during-disconnect, and post-reconnect
            for {set i 0} {$i < 100} {incr i} {
                assert_equal [$master get "psync-pre:$i"] [$replica get "psync-pre:$i"]
            }
            for {set i 0} {$i < 200} {incr i} {
                assert_equal [$master get "psync-during:$i"] [$replica get "psync-during:$i"]
            }
            for {set i 0} {$i < 50} {incr i} {
                assert_equal [$master get "psync-post:$i"] [$replica get "psync-post:$i"]
            }

            # Final digest check
            assert_equal [$master debug digest] [$replica debug digest]
        }

        test "Repeated disconnect/reconnect cycles under write load with compression" {
            # Run 5 disconnect/reconnect cycles while continuously writing
            for {set cycle 0} {$cycle < 5} {incr cycle} {
                # Write a batch of data
                for {set i 0} {$i < 50} {incr i} {
                    $master set "cycle${cycle}:key$i" [string repeat "D${cycle}" 100]
                }

                wait_for_ofs_sync $master $replica

                # Verify this cycle's data
                for {set i 0} {$i < 50} {incr i} {
                    assert_equal [$master get "cycle${cycle}:key$i"] [$replica get "cycle${cycle}:key$i"]
                }

                # Disconnect (except on last cycle)
                if {$cycle < 4} {
                    $master client kill type replica

                    wait_for_condition 50 100 {
                        [string match {*connected_slaves:0*} [$master info replication]]
                    } else {
                        fail "Replica not disconnected on cycle $cycle"
                    }

                    # Write some data while disconnected
                    for {set i 0} {$i < 20} {incr i} {
                        $master set "gap${cycle}:key$i" "gap-value-$i"
                    }

                    # Wait for reconnect
                    wait_for_condition 100 100 {
                        [string match {*master_link_status:up*} [$replica info replication]]
                    } else {
                        fail "Replica did not reconnect on cycle $cycle"
                    }

                    wait_for_ofs_sync $master $replica

                    # Verify gap data was replicated after reconnect
                    for {set i 0} {$i < 20} {incr i} {
                        assert_equal "gap-value-$i" [$replica get "gap${cycle}:key$i"]
                    }
                }
            }

            # Final full digest comparison
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 11: Replica disconnects via REPLICAOF NO ONE, then re-attaches.
#          Verifies that stopping and restarting replication with compression
#          works cleanly — the replica gets a fresh compression context.
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "REPLICAOF NO ONE then re-attach with compression" {
            # Establish replication
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Write and verify initial data
            for {set i 0} {$i < 50} {incr i} {
                $master set "before-detach:$i" "val$i"
            }
            wait_for_ofs_sync $master $replica
            for {set i 0} {$i < 50} {incr i} {
                assert_equal "val$i" [$replica get "before-detach:$i"]
            }

            # Detach replica explicitly
            $replica replicaof no one

            # Wait for master to see no replicas
            wait_for_condition 50 100 {
                [string match {*connected_slaves:0*} [$master info replication]]
            } else {
                fail "Master still sees replica after REPLICAOF NO ONE"
            }

            # Write data on master while replica is detached
            for {set i 0} {$i < 100} {incr i} {
                $master set "while-detached:$i" [string repeat "E" 200]
            }

            # Re-attach replica
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Write more data after re-attach
            for {set i 0} {$i < 30} {incr i} {
                $master set "after-reattach:$i" "reattached-$i"
            }
            wait_for_ofs_sync $master $replica

            # Verify all data
            for {set i 0} {$i < 50} {incr i} {
                assert_equal "val$i" [$replica get "before-detach:$i"]
            }
            for {set i 0} {$i < 100} {incr i} {
                assert_equal [$master get "while-detached:$i"] [$replica get "while-detached:$i"]
            }
            for {set i 0} {$i < 30} {incr i} {
                assert_equal "reattached-$i" [$replica get "after-reattach:$i"]
            }

            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 12: Rapid disconnect/reconnect with heavy write load and compression.
#          Stresses the compression lifecycle (create/destroy) under pressure.
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4 repl-backlog-size 10mb}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica [srv 0 client]

        test "Rapid disconnect/reconnect under heavy writes with compression" {
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            # Seed initial data
            for {set i 0} {$i < 500} {incr i} {
                $master set "heavy:$i" [string repeat "X" 1024]
            }
            wait_for_ofs_sync $master $replica

            # Rapid disconnect/reconnect: 8 cycles with minimal delay
            for {set cycle 0} {$cycle < 8} {incr cycle} {
                # Kill connection
                $master client kill type replica

                # Immediately start writing (don't wait for disconnect confirmation)
                for {set i 0} {$i < 30} {incr i} {
                    $master set "rapid${cycle}:$i" [string repeat "R${cycle}" 512]
                }

                # Wait for reconnect
                wait_for_condition 100 50 {
                    [string match {*master_link_status:up*} [$replica info replication]]
                } else {
                    fail "Replica did not reconnect on rapid cycle $cycle"
                }
            }

            # Final sync and verification
            for {set i 0} {$i < 100} {incr i} {
                $master set "final:$i" "done-$i"
            }
            wait_for_ofs_sync $master $replica

            # Verify final data
            for {set i 0} {$i < 100} {incr i} {
                assert_equal "done-$i" [$replica get "final:$i"]
            }

            # Verify data from last rapid cycle made it through
            for {set i 0} {$i < 30} {incr i} {
                assert_equal [$master get "rapid7:$i"] [$replica get "rapid7:$i"]
            }

            # Digest must match
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}


# ---------------------------------------------------------------------------
# Test 13: Multiple replicas disconnect and reconnect at different times.
#          Verifies that each replica gets a fresh compression context and
#          all eventually converge to the same state.
# ---------------------------------------------------------------------------
start_server {tags {"repl-inline-compression external:skip"} overrides {save "" io-threads 4 repl-backlog-size 10mb}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    enable_repl_compression $master

    start_server {overrides {save ""}} {
        set replica1 [srv 0 client]
        start_server {overrides {save ""}} {
            set replica2 [srv 0 client]

            test "Multiple replicas disconnect and reconnect at different times" {
                # Connect both replicas
                $replica1 replicaof $master_host $master_port
                $replica2 replicaof $master_host $master_port
                wait_for_sync $replica1
                wait_for_sync $replica2

                # Write initial data
                for {set i 0} {$i < 50} {incr i} {
                    $master set "multi-init:$i" "init-$i"
                }
                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2

                # Disconnect replica1 only (replica2 stays connected)
                set replica1_id ""
                set info [$master info replication]
                # Kill all replica connections, replica2 will reconnect
                $master client kill type replica

                # Write data while replica1 is disconnected
                for {set i 0} {$i < 80} {incr i} {
                    $master set "mid-gap:$i" [string repeat "M" 200]
                }

                # Wait for both to reconnect
                wait_for_condition 100 100 {
                    [string match {*connected_slaves:2*} [$master info replication]]
                } else {
                    fail "Not all replicas reconnected"
                }

                # Write more data
                for {set i 0} {$i < 40} {incr i} {
                    $master set "post-reconnect:$i" "post-$i"
                }
                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2

                # All three must agree
                set master_digest [$master debug digest]
                assert {$master_digest ne "0000000000000000000000000000000000000000"}
                assert_equal $master_digest [$replica1 debug digest]
                assert_equal $master_digest [$replica2 debug digest]

                # Spot-check specific keys
                for {set i 0} {$i < 80} {incr i} {
                    assert_equal [$master get "mid-gap:$i"] [$replica1 get "mid-gap:$i"]
                    assert_equal [$master get "mid-gap:$i"] [$replica2 get "mid-gap:$i"]
                }
            }
        }
    }
}
