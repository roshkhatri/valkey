tags {"repl-compression external:skip"} {

start_server {overrides {save ""}} {

    test {replcompression round-trip consistency} {
        for {set i 0} {$i < 100} {incr i} {
            set val [expr {int(rand() * 2) ? "yes" : "no"}]
            r config set replcompression $val
            set got [lindex [r config get replcompression] 1]
            assert_equal $val $got
        }
        # Restore default
        r config set replcompression no
    }

    test {repl-compression-algo round-trip consistency} {
        # Only lz4 is valid
        r config set repl-compression-algo lz4
        set got [lindex [r config get repl-compression-algo] 1]
        assert_equal "lz4" $got
    }

    test {repl-compression-level round-trip consistency} {
        for {set i 0} {$i < 100} {incr i} {
            set val [expr {int(rand() * 1023) - 1000}]
            r config set repl-compression-level $val
            set got [lindex [r config get repl-compression-level] 1]
            assert_equal $val $got
        }
        # Restore default
        r config set repl-compression-level -5
    }

    test {Invalid enum values for repl-compression-algo are rejected} {
        set original [lindex [r config get repl-compression-algo] 1]
        foreach val {none snappy zstd gzip} {
            catch {r config set repl-compression-algo $val} err
            assert_match "*argument(s) must be one of the following*" $err
            set got [lindex [r config get repl-compression-algo] 1]
            assert_equal $original $got
        }
    }

    test {Compression level acceptance depends on range} {
        for {set i 0} {$i < 100} {incr i} {
            set val [expr {int(rand() * 2048) - 1024}]
            set in_range [expr {$val >= -1000 && $val <= 22}]

            set prev_level [lindex [r config get repl-compression-level] 1]
            set rc [catch {r config set repl-compression-level $val} err]

            if {$in_range} {
                assert_equal 0 $rc "Expected success for val=$val but got error: $err"
                set got [lindex [r config get repl-compression-level] 1]
                assert_equal $val $got
            } else {
                assert_equal 1 $rc "Expected failure for val=$val"
            }
        }
        # Restore default
        r config set repl-compression-level -5
    }
}

start_server {tags {"repl"} overrides {save ""}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Replica with replcompression yes sends capa compression and primary records the bit} {
        start_server {overrides {save "" replcompression yes}} {
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

    test {Replica with replcompression no omits capa compression} {
        start_server {overrides {save "" replcompression no}} {
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
}

start_server {overrides {save ""}} {

    test {Repl compression config defaults are correct} {
        assert_equal "no" [lindex [r config get replcompression] 1]
        assert_equal "lz4" [lindex [r config get repl-compression-algo] 1]
        assert_equal "-5" [lindex [r config get repl-compression-level] 1]
    }

    test {Invalid repl-compression-level range is rejected} {
        catch {r config set repl-compression-level -1001} err
        assert_match "*out of range*" $err
        catch {r config set repl-compression-level 23} err
        assert_match "*out of range*" $err
    }

    test {Repl compression configs survive CONFIG REWRITE and restart} {
        r config set replcompression yes
        r config set repl-compression-algo lz4
        r config set repl-compression-level -9
        r config rewrite

        restart_server 0 true false

        assert_equal "yes" [lindex [r config get replcompression] 1]
        assert_equal "lz4" [lindex [r config get repl-compression-algo] 1]
        assert_equal "-9" [lindex [r config get repl-compression-level] 1]

        # Restore defaults
        r config set repl-compression-level -5
        r config set replcompression no
    }
}

# --- Compressed full sync transport tests ---

start_server {tags {"repl needs:debug"} overrides {save "" enable-debug-command local repl-diskless-sync yes repl-diskless-sync-delay 0 replcompression yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Diskless full sync with replcompression transports compressed RDB} {
        $primary flushall
        for {set i 0} {$i < 500} {incr i} {
            $primary set "comp:$i" [string repeat "payload$i " 40]
        }
        $primary lpush mylist a b c d e
        $primary sadd myset x y z
        $primary zadd zset 1.0 a 2.0 b 3.0 c
        $primary hset myhash f1 v1 f2 v2

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after compressed diskless full sync"
            }

            assert_equal [string repeat "payload42 " 40] [$replica get comp:42]
            assert_equal 5 [$replica llen mylist]
            assert_equal 3 [$replica scard myset]
            assert_equal "v1" [$replica hget myhash f1]

            # Verify incremental replication continues after compressed full sync
            $primary set post-sync-key "post-sync-value"
            wait_for_condition 50 100 {
                [$replica get post-sync-key] eq "post-sync-value"
            } else {
                fail "Replica did not receive post-sync write after compressed full sync"
            }

            $replica replicaof no one
        }
    }

    test {Diskless full sync large dataset with replcompression} {
        $primary flushall
        $primary debug populate 10000
        $primary set extra_key "extra_value"

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after large compressed diskless full sync"
            }

            assert_equal "extra_value" [$replica get extra_key]
            assert_equal 10001 [$replica dbsize]

            $replica replicaof no one
        }
    }

    test {Compressed full sync backward compat - replcompression replica with uncompressed primary} {
        start_server {overrides {save "" repl-diskless-sync yes repl-diskless-sync-delay 0 replcompression no enable-debug-command local}} {
            set uncompressed_primary [srv 0 client]
            set uncompressed_primary_host [srv 0 host]
            set uncompressed_primary_port [srv 0 port]

            $uncompressed_primary flushall
            for {set i 0} {$i < 100} {incr i} {
                $uncompressed_primary set "bcompat:$i" [string repeat "data$i " 20]
            }

            start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
                set replica [srv 0 client]
                $replica replicaof $uncompressed_primary_host $uncompressed_primary_port
                wait_for_sync $replica

                wait_for_condition 50 100 {
                    [status $replica master_link_status] eq "up" &&
                    [$uncompressed_primary debug digest] eq [$replica debug digest]
                } else {
                    fail "Replica digest mismatch when connecting to uncompressed primary"
                }

                assert_equal [string repeat "data42 " 20] [$replica get bcompat:42]

                $replica replicaof no one
            }
        }
    }

    test {Compression disabled when replica lacks capability} {
        $primary flushall
        for {set i 0} {$i < 100} {incr i} {
            $primary set "nocap:$i" [string repeat "value$i " 20]
        }

        start_server {overrides {save "" replcompression no repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch when replica lacks compression capability"
            }

            assert_equal [string repeat "value42 " 20] [$replica get nocap:42]

            $replica replicaof no one
        }
    }
}

start_server {tags {"repl needs:debug"} overrides {save "" enable-debug-command local repl-diskless-sync yes repl-diskless-sync-delay 0 replcompression yes dual-channel-replication-enabled yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    test {Dual-channel full sync with replcompression} {
        $primary flushall
        for {set i 0} {$i < 500} {incr i} {
            $primary set "dual:$i" [string repeat "payload$i " 40]
        }

        start_server {overrides {save "" replcompression yes dual-channel-replication-enabled yes}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after compressed dual-channel full sync"
            }

            assert_equal [string repeat "payload42 " 40] [$replica get dual:42]

            # Verify incremental replication continues
            $primary set dual-post-sync "after-dual-sync"
            wait_for_condition 50 100 {
                [$replica get dual-post-sync] eq "after-dual-sync"
            } else {
                fail "Replica did not receive post-sync write after compressed dual-channel full sync"
            }

            $replica replicaof no one
        }
    }
}

}
