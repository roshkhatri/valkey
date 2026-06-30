source tests/support/aofmanifest.tcl

proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

# Test that reuses the RDB file from full sync as the AOF base file
# when aof-use-rdb-preamble is enabled and disk-based replication is used.

proc get_aof_manifest_path {r} {
    set dir [lindex [$r config get dir] 1]
    set appenddirname [lindex [$r config get appenddirname] 1]
    set appendfilename [lindex [$r config get appendfilename] 1]
    return [file join $dir $appenddirname $appendfilename$::manifest_suffix]
}

# The RDB-reuse-as-AOF-base tests below assert plaintext disk-based full-sync
# behavior, which streaming compression legitimately changes: a compressed
# full-sync RDB cannot be reused as an AOF base (the replica correctly falls back
# to BGREWRITEAOF, covered by the dedicated test in the repl-compression block at
# the bottom of this file). So this block is NOT tagged repl-compression and runs
# only in the regular (uncompressed) job.
tags {"repl external:skip"} {

    # Test 1: Disk-based full sync with aof-use-rdb-preamble yes should
    # reuse the RDB file as AOF base file
    test "Reuse RDB from disk-based full sync as AOF base file" {
        start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save ""}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 100} {incr i} {
                $primary set "key:$i" "value:$i"
            }
            waitForBgrewriteaof $primary

            start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                # Start replication
                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                # Check AOF state is ON (not WAIT_REWRITE)
                wait_for_condition 50 100 {
                    [string match "*aof_rewrite_in_progress:0*" [$replica info persistence]]
                } else {
                    fail "AOF rewrite still in progress"
                }

                # Verify the log message about reusing RDB
                wait_for_condition 50 100 {
                    [log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]
                } else {
                    fail "Expected log message about reusing RDB file not found"
                }

                # Verify AOF base file exists and is RDB format
                set manifest_path [get_aof_manifest_path $replica]
                set base_name [get_cur_base_aof_name $manifest_path]
                assert {$base_name ne ""}
                assert {[string match "*.rdb" $base_name]}

                # Verify data integrity
                assert_equal 100 [$replica dbsize]
                for {set i 0} {$i < 100} {incr i} {
                    assert_equal "value:$i" [$replica get "key:$i"]
                }
            }
        }
    }

    # Test 2: After sync, new writes should go to AOF incr file
    test "New writes after RDB-reuse sync go to AOF incr file" {
        start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save ""}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 50} {incr i} {
                $primary set "key:$i" "value:$i"
            }
            waitForBgrewriteaof $primary

            start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                wait_for_condition 50 100 {
                    [log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]
                } else {
                    fail "Expected log message not found"
                }

                # Write new data to primary after sync
                for {set i 50} {$i < 100} {incr i} {
                    $primary set "key:$i" "value:$i"
                }

                # Wait for replication to propagate
                wait_for_ofs_sync $primary $replica

                # Verify incr AOF file exists
                set manifest_path [get_aof_manifest_path $replica]
                set incr_name [get_last_incr_aof_name $manifest_path]
                assert {$incr_name ne ""}

                # Verify all data is present
                assert_equal 100 [$replica dbsize]
                for {set i 0} {$i < 100} {incr i} {
                    assert_equal "value:$i" [$replica get "key:$i"]
                }
            }
        }
    }

    # Test 3: Replica restart should load AOF correctly after RDB-reuse sync
    test "Replica restart loads AOF correctly after RDB-reuse sync" {
        start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save ""}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 50} {incr i} {
                $primary set "key:$i" "value:$i"
            }
            waitForBgrewriteaof $primary

            start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                wait_for_condition 50 100 {
                    [log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]
                } else {
                    fail "Expected log message not found"
                }

                # Write more data
                for {set i 50} {$i < 80} {incr i} {
                    $primary set "key:$i" "value:$i"
                }
                wait_for_ofs_sync $primary $replica

                # Stop replica and disconnect from primary
                $replica replicaof no one

                # Restart replica (this tests AOF loading)
                restart_server 0 true false
                set replica [srv 0 client]
                wait_done_loading $replica

                # Verify data integrity after restart
                assert_equal 80 [$replica dbsize]
                for {set i 0} {$i < 80} {incr i} {
                    assert_equal "value:$i" [$replica get "key:$i"]
                }
            }
        }
    }

    test "Replica restart reuses disk-based sync RDB when primary rdbcompression is lz4-stream" {
        start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save "" rdbcompression lz4-stream}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 40} {incr i} {
                $primary set "lz4-key:$i" "value:$i"
            }
            waitForBgrewriteaof $primary

            start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                wait_for_condition 50 100 {
                    [log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]
                } else {
                    fail "Expected log message about reusing RDB file not found"
                }

                assert {![log_file_matches $replica_log "*uses streaming compression, falling back to BGREWRITEAOF*"]}
                set manifest_path [get_aof_manifest_path $replica]
                set base_name [get_cur_base_aof_name $manifest_path]
                assert {$base_name ne ""}
                assert {[string match "*.rdb" $base_name]}

                for {set i 40} {$i < 60} {incr i} {
                    $primary set "lz4-key:$i" "value:$i"
                }
                wait_for_ofs_sync $primary $replica

                $replica replicaof no one

                restart_server 0 true false
                set replica [srv 0 client]
                wait_done_loading $replica

                assert_equal 60 [$replica dbsize]
                for {set i 0} {$i < 60} {incr i} {
                    assert_equal "value:$i" [$replica get "lz4-key:$i"]
                }
            }
        }
    }

}

# These tests are compatible with streaming compression and stay in the
# repl-compression matrix: the BGREWRITEAOF-fallback test below verifies the
# correct compressed-RDB behavior, and the remaining tests assert
# fallback/diskless paths that do not depend on plaintext RDB reuse.
tags {"repl external:skip" repl-compression} {

    # A streaming-compressed disk-based sync RDB (repl-compression lz4-stream on
    # both ends) cannot be reused as an AOF base, so the replica falls back to
    # BGREWRITEAOF. Inverse of the rdbcompression test above (plaintext RDB reused).
    test "Disk-based full sync with repl-compression lz4-stream falls back to BGREWRITEAOF for AOF base" {
        start_server {overrides {repl-diskless-sync no repl-compression lz4-stream save ""}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 40} {incr i} {
                $primary set "rcomp-key:$i" "value:$i"
            }

            start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync no repl-compression lz4-stream save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                # Replica detects the compressed sync RDB and falls back to BGREWRITEAOF.
                wait_for_condition 50 100 {
                    [log_file_matches $replica_log "*uses streaming compression, falling back to BGREWRITEAOF*"]
                } else {
                    fail "Expected streaming-compression AOF fallback log not found"
                }

                # And it must NOT have reused the sync RDB as the AOF base.
                assert {![log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]}

                # AOF comes up via BGREWRITEAOF; a base file must exist.
                waitForBgrewriteaof $replica
                set manifest_path [get_aof_manifest_path $replica]
                set base_name [get_cur_base_aof_name $manifest_path]
                assert {$base_name ne ""}

                # Data correct at runtime (loaded from the compressed socket stream).
                assert_equal 40 [$replica dbsize]
                for {set i 0} {$i < 40} {incr i} {
                    assert_equal "value:$i" [$replica get "rcomp-key:$i"]
                }

                # After restart: AOF loads from the rewritten base, not the compressed RDB.
                $replica replicaof no one
                restart_server 0 true false
                set replica [srv 0 client]
                wait_done_loading $replica

                assert_equal 40 [$replica dbsize]
                for {set i 0} {$i < 40} {incr i} {
                    assert_equal "value:$i" [$replica get "rcomp-key:$i"]
                }
            }
        }
    }

    # Test 4: aof-use-rdb-preamble no should fall back to bgrewriteaof
    test "Disk-based sync with aof-use-rdb-preamble no uses bgrewriteaof" {
        start_server {overrides {appendonly yes aof-use-rdb-preamble no repl-diskless-sync no save ""}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 50} {incr i} {
                $primary set "key:$i" "value:$i"
            }

            start_server {overrides {appendonly yes aof-use-rdb-preamble no repl-diskless-sync no save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                # Should NOT see the RDB reuse log message
                after 1000
                assert {![log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]}

                # Structural assertion: AOF base exists (produced by bgrewriteaof)
                # and uses .aof suffix (not .rdb) since rdb-preamble is off.
                waitForBgrewriteaof $replica
                set manifest_path [get_aof_manifest_path $replica]
                set base_name [get_cur_base_aof_name $manifest_path]
                assert {$base_name ne ""}
                assert {[string match "*.aof" $base_name]}

                assert_equal 50 [$replica dbsize]
                for {set i 0} {$i < 50} {incr i} {
                    assert_equal "value:$i" [$replica get "key:$i"]
                }
            }
        }
    }

    # Test 5: Diskless sync with aof-use-rdb-preamble yes should fall back
    # to bgrewriteaof (no RDB file on disk to reuse)
    test "Diskless sync with aof-use-rdb-preamble yes uses bgrewriteaof fallback" {
        start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync yes repl-diskless-sync-delay 0 save ""}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 50} {incr i} {
                $primary set "key:$i" "value:$i"
            }

            start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-load flush-before-load save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                after 1000
                assert {![log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]}

                # Structural assertion: bgrewriteaof should produce an AOF base
                # file with .rdb suffix (since rdb-preamble is yes).
                waitForBgrewriteaof $replica
                set manifest_path [get_aof_manifest_path $replica]
                set base_name [get_cur_base_aof_name $manifest_path]
                assert {$base_name ne ""}
                assert {[string match "*.rdb" $base_name]}

                assert_equal 50 [$replica dbsize]
                for {set i 0} {$i < 50} {incr i} {
                    assert_equal "value:$i" [$replica get "key:$i"]
                }
            }
        }
    }

    # Test 6: Diskless sync with a stale local RDB must NOT reuse it.
    # This verifies that disk_based_sync=0 prevents the optimization even
    # when a leftover dump.rdb exists on the replica.
    test "Diskless sync with stale local RDB does not reuse it as AOF base" {
        start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-sync yes repl-diskless-sync-delay 0 save ""}} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]

            for {set i 0} {$i < 50} {incr i} {
                $primary set "key:$i" "value:$i"
            }

            start_server {overrides {appendonly yes aof-use-rdb-preamble yes repl-diskless-load flush-before-load save ""}} {
                set replica [srv 0 client]
                set replica_log [srv 0 stdout]

                # Create stale data and persist it as a local RDB so that
                # dump.rdb exists when the diskless sync completes.
                $replica set stale_key stale_value
                $replica bgsave
                waitForBgsave $replica

                # Now do a diskless full sync
                $replica replicaof $primary_host $primary_port
                wait_for_sync $replica

                # The stale RDB must NOT have been reused
                after 1000
                assert {![log_file_matches $replica_log "*Reused RDB file from primary sync as AOF base file*"]}

                # Structural assertion: bgrewriteaof produced the base file
                waitForBgrewriteaof $replica
                set manifest_path [get_aof_manifest_path $replica]
                set base_name [get_cur_base_aof_name $manifest_path]
                assert {$base_name ne ""}

                # Data must come from the primary, not the stale RDB
                assert_equal 50 [$replica dbsize]
                for {set i 0} {$i < 50} {incr i} {
                    assert_equal "value:$i" [$replica get "key:$i"]
                }
                assert_equal "" [$replica get stale_key]

                # Restart replica and verify data again. This catches the regression
                # where a stale dump.rdb was incorrectly reused as AOF base: at
                # runtime memory is correct (from socket), but after restart the
                # wrong AOF base would load stale data.
                $replica replicaof no one
                restart_server 0 true false
                set replica [srv 0 client]
                wait_done_loading $replica

                assert_equal 50 [$replica dbsize]
                for {set i 0} {$i < 50} {incr i} {
                    assert_equal "value:$i" [$replica get "key:$i"]
                }
                assert_equal "" [$replica get stale_key]
            }
        }
    }
}
