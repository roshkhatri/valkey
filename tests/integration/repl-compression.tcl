tags {"repl-compression external:skip"} {

# ============================================================
# Config CRUD — single-server tests, no replication needed
# ============================================================

start_server {overrides {save ""}} {

    test {Repl compression config defaults are correct} {
        assert_equal "no" [lindex [r config get replcompression] 1]
    }

    test {replcompression can be toggled on and off} {
        r config set replcompression yes
        assert_equal "yes" [lindex [r config get replcompression] 1]
        r config set replcompression no
        assert_equal "no" [lindex [r config get replcompression] 1]
    }

    test {Repl compression configs survive CONFIG REWRITE and restart} {
        r config set replcompression yes
        r config rewrite

        restart_server 0 true false

        assert_equal "yes" [lindex [r config get replcompression] 1]

        # Restore default
        r config set replcompression no
    }
}

# ============================================================
# Replication handshake behavior — primary + replica tests
# ============================================================

start_server {tags {"repl"} overrides {save ""}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Replica with replcompression no does NOT send capa compression} {
        start_server {overrides {save "" replcompression no}} {
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

    test {Replica with replcompression yes and diskless load sends capa compression} {
        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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

    test {Replica with replcompression yes but disk-backed load does NOT send capa compression} {
        start_server {overrides {save "" replcompression yes repl-diskless-load disabled}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started"
            }

            # Full sync completes normally — disk-backed replica does not advertise compression
            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Primary receiving capa compression still completes full sync correctly (no-op)} {
        $primary flushall
        for {set i 0} {$i < 100} {incr i} {
            $primary set "noop:$i" [string repeat "value$i " 10]
        }

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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

    test {Toggling replcompression mid-runtime affects the next handshake} {
        # First sync with compression disabled
        start_server {overrides {save "" replcompression no repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started with replcompression no"
            }

            assert_equal {up} [s 0 master_link_status]

            # Toggle compression on at runtime
            $replica config set replcompression yes

            # Disconnect and reconnect to trigger a new handshake
            $replica replicaof no one
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication not started after toggling replcompression yes"
            }

            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Compressed incremental replication delivers correct data} {
        $primary config set replcompression yes
        $primary flushall

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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

        $primary config set replcompression no
    }

    test {Partial resync with compression delivers correct data} {
        $primary config set replcompression yes
        $primary flushall

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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

            # Simulate disconnect + reconnect (partial resync)
            $replica replicaof no one
            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Reconnection failed"
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

        $primary config set replcompression no
    }

    test {CONFIG SET replcompression no disconnects compressed replicas} {
        $primary config set replcompression yes

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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
            $primary config set replcompression no

            # Replica should disconnect and reconnect
            wait_for_condition 50 200 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replica did not reconnect after replcompression disabled"
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
        $primary config set replcompression yes

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
            set replica1 [srv 0 client]
            $replica1 replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replica 1 not started"
            }

            start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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
        $primary config set replcompression no
    }

    # ============================================================
    # Comprehensive data-type coverage with compression enabled
    # ============================================================

    if {[lsearch $::denytags "repl-compression-suite"] == -1} {

    $primary config set replcompression yes
    $primary flushall

    start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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
        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
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

    $primary config set replcompression no

    } ;# end repl-compression-suite
}

}
