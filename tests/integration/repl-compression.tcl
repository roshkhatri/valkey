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
            assert {
                [string match "*repl-compression-algo*" $err] ||
                [string match "*argument(s) must be one of the following*" $err]
            }
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

    test {Primary-side replcompression does not force compression onto replicas without capa compression} {
        $primary config set replcompression yes

        start_server {overrides {save "" replcompression no}} {
            set replica [srv 0 client]
            for {set i 0} {$i < 20} {incr i} {
                $primary set "plain:$i" [string repeat "value-$i-" 24]
            }

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 100 100 {
                [$replica get "plain:19"] eq [$primary get "plain:19"]
            } else {
                fail "Replica did not stay in sync with primary-side replcompression enabled"
            }

            assert_equal [$primary debug digest] [$replica debug digest]
            $replica replicaof no one
        }

        $primary config set replcompression no
    }
}

start_server {tags {"repl external:skip"} overrides {save "" io-threads 4 repl-diskless-sync no replcompression yes repl-compression-algo lz4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    start_server {overrides {save "" replcompression yes repl-compression-algo lz4}} {
        set replica [srv 0 client]
        set replica_log [srv 0 stdout]

        test {End-to-end compressed replication preserves data across full sync and incremental writes} {
            for {set i 0} {$i < 50} {incr i} {
                $master set "pre:$i" [string repeat "value-$i-" 32]
            }
            $master hset pre:hash f1 v1 f2 v2 f3 v3
            $master sadd pre:set a b c d

            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            for {set i 0} {$i < 50} {incr i} {
                assert_equal [$master get "pre:$i"] [$replica get "pre:$i"]
            }
            assert_equal [$master hgetall pre:hash] [$replica hgetall pre:hash]
            assert_equal [lsort [$master smembers pre:set]] [lsort [$replica smembers pre:set]]

            for {set i 0} {$i < 50} {incr i} {
                $master set "post:$i" [string repeat "payload-$i-" 48]
            }
            $master lpush post:list x y z
            $master zadd post:zset 1 one 2 two 3 three

            wait_for_condition 100 100 {
                [$replica get "post:49"] eq [$master get "post:49"]
            } else {
                fail "Replica did not receive incremental compressed replication data in time"
            }

            for {set i 0} {$i < 50} {incr i} {
                assert_equal [$master get "post:$i"] [$replica get "post:$i"]
            }
            assert_equal [$master lrange post:list 0 -1] [$replica lrange post:list 0 -1]
            assert_equal [$master zrange post:zset 0 -1 withscores] [$replica zrange post:zset 0 -1 withscores]

            set master_digest [$master debug digest]
            set replica_digest [$replica debug digest]
            assert {$master_digest ne "0000000000000000000000000000000000000000"}
            assert_equal $master_digest $replica_digest

            $replica replicaof no one
        }

        test {Compressed partial resync recreates the stream and preserves backlog data} {
            set loglines [count_log_lines 0]
            set replica_disconnects_before [count_message_lines $replica_log "Connection with primary lost."]

            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            for {set i 0} {$i < 40} {incr i} {
                $master set "psync-pre:$i" [string repeat "before-$i-" 24]
            }

            wait_for_condition 100 100 {
                [$replica get "psync-pre:39"] eq [$master get "psync-pre:39"]
            } else {
                fail "Replica did not catch up before disconnect"
            }

            $replica client kill $master_host:$master_port

            wait_for_condition 100 100 {
                [count_message_lines $replica_log "Connection with primary lost."] > $replica_disconnects_before
            } else {
                fail "Master did not observe replica disconnect"
            }

            for {set i 0} {$i < 80} {incr i} {
                $master set "psync-gap:$i" [string repeat "gap-$i-" 32]
            }

            wait_for_log_messages 0 {"*Successful partial resynchronization with primary*"} $loglines 100 100

            wait_for_condition 100 100 {
                [$replica get "psync-gap:79"] eq [$master get "psync-gap:79"]
            } else {
                fail "Replica did not receive backlog data after compressed partial resync"
            }

            for {set i 0} {$i < 20} {incr i} {
                $master set "psync-post:$i" [string repeat "after-$i-" 28]
            }

            wait_for_condition 100 100 {
                [$replica get "psync-post:19"] eq [$master get "psync-post:19"]
            } else {
                fail "Replica did not catch up after compressed partial resync"
            }

            for {set i 0} {$i < 40} {incr i} {
                assert_equal [$master get "psync-pre:$i"] [$replica get "psync-pre:$i"]
            }
            for {set i 0} {$i < 80} {incr i} {
                assert_equal [$master get "psync-gap:$i"] [$replica get "psync-gap:$i"]
            }
            for {set i 0} {$i < 20} {incr i} {
                assert_equal [$master get "psync-post:$i"] [$replica get "psync-post:$i"]
            }

            assert_equal [$master debug digest] [$replica debug digest]

            $replica replicaof no one
        }

        test {Disabling replcompression disconnects compressed replicas and replication recovers} {
            set replica_disconnects_before [count_message_lines $replica_log "Connection with primary lost."]
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            $master set runtime:disable before
            wait_for_condition 100 100 {
                [$replica get "runtime:disable"] eq "before"
            } else {
                fail "Replica did not catch up before disabling replcompression"
            }

            assert_match {*connected_slaves:1*} [$master info replication]

            $master config set replcompression no

            wait_for_condition 100 100 {
                [count_message_lines $replica_log "Connection with primary lost."] > $replica_disconnects_before
            } else {
                fail "Compressed replica was not disconnected after replcompression was disabled"
            }

            wait_for_condition 100 100 {
                [string match {*master_link_status:up*} [$replica info replication]] &&
                [string match {*connected_slaves:1*} [$master info replication]]
            } else {
                fail "Replica did not reconnect after replcompression was disabled"
            }

            $master set runtime:disable after
            wait_for_condition 100 100 {
                [$replica get "runtime:disable"] eq "after"
            } else {
                fail "Replica did not resume replication after reconnecting uncompressed"
            }

            $replica replicaof no one
            $master config set replcompression yes
        }
    }
}

start_server {tags {"repl external:skip"} overrides {save "" io-threads 4 repl-diskless-sync yes repl-diskless-sync-delay 0 dual-channel-replication-enabled yes replcompression yes repl-compression-algo lz4}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master_log [srv 0 stdout]

    start_server {overrides {save "" repl-diskless-load swapdb dual-channel-replication-enabled yes replcompression yes repl-compression-algo lz4}} {
        set replica [srv 0 client]

        test {Dual-channel full sync transitions into compressed incremental replication} {
            for {set i 0} {$i < 40} {incr i} {
                $master set "dual:rdb:$i" [string repeat "seed-$i-" 36]
            }

            set dual_sync_logs_before [count_message_lines $master_log "using: dual-channel"]
            set compression_logs_before [count_message_lines $master_log "Replication compression enabled for replica"]
            $replica replicaof $master_host $master_port
            wait_for_sync $replica

            wait_for_condition 100 100 {
                [count_message_lines $master_log "using: dual-channel"] > $dual_sync_logs_before &&
                [count_message_lines $master_log "Replication compression enabled for replica"] > $compression_logs_before
            } else {
                fail "Master did not log dual-channel sync with replication compression"
            }

            for {set i 0} {$i < 40} {incr i} {
                assert_equal [$master get "dual:rdb:$i"] [$replica get "dual:rdb:$i"]
            }

            for {set i 0} {$i < 50} {incr i} {
                $master set "dual:stream:$i" [string repeat "tail-$i-" 44]
            }
            $master hset dual:hash f1 one f2 two

            wait_for_condition 100 100 {
                [$replica get "dual:stream:49"] eq [$master get "dual:stream:49"]
            } else {
                fail "Replica did not receive compressed incremental data after dual-channel full sync"
            }

            assert_equal [$master hgetall dual:hash] [$replica hgetall dual:hash]
            assert_equal [$master debug digest] [$replica debug digest]
            $replica replicaof no one
        }

        test {Dual-channel full sync decodes compressed buffered incremental data after RDB load} {
            $master flushall
            $master config set rdb-key-save-delay 200
            $replica config set key-load-delay 100
            for {set i 0} {$i < 300} {incr i} {
                $master set "dual:buffer:rdb:$i" [string repeat "seed-$i-" 48]
            }

            set compression_logs_before [count_message_lines $master_log "Replication compression enabled for replica"]
            $replica replicaof $master_host $master_port

            wait_for_condition 100 100 {
                [string match "*state=wait_bgsave*" [$master info replication]] ||
                [string match "*state=bg_transfer*" [$master info replication]] ||
                [string match "*state=send_bulk*" [$master info replication]]
            } else {
                $master config set rdb-key-save-delay 0
                $replica config set key-load-delay 0
                fail "Replica did not enter bgsave/transfer during dual-channel full sync"
            }

            wait_for_condition 100 100 {
                [count_message_lines $master_log "Replication compression enabled for replica"] > $compression_logs_before
            } else {
                $master config set rdb-key-save-delay 0
                $replica config set key-load-delay 0
                fail "Master did not enable compressed incremental replication during dual-channel full sync"
            }

            for {set i 0} {$i < 25} {incr i} {
                $master set "dual:buffer:stream:$i" [string repeat "tail-$i-" 40]
            }
            $master hset dual:buffer:hash f1 one f2 two

            wait_for_sync $replica
            $master config set rdb-key-save-delay 0
            $replica config set key-load-delay 0

            wait_for_condition 100 100 {
                [$replica get "dual:buffer:stream:24"] eq [$master get "dual:buffer:stream:24"]
            } else {
                fail "Replica did not apply compressed buffered incremental data after dual-channel full sync"
            }

            assert_equal [$master hgetall dual:buffer:hash] [$replica hgetall dual:buffer:hash]
            assert_equal [$master debug digest] [$replica debug digest]
            $replica replicaof no one
        }
    }
}

start_server {tags {"repl needs:debug"} overrides {save "" enable-debug-command local repl-diskless-sync yes repl-diskless-sync-delay 0 replcompression yes}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set primary_log [srv 0 stdout]

    # The tests below exercise broader transport compatibility behavior that
    # this branch relies on but does not redefine as its primary scope.
    test {Transport compatibility: diskless full sync with replcompression transports compressed RDB} {
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

            $primary set post-sync-key "post-sync-value"
            wait_for_condition 50 100 {
                [$replica get post-sync-key] eq "post-sync-value"
            } else {
                fail "Replica did not receive post-sync write after compressed full sync"
            }

            $replica replicaof no one
        }
    }

    test {Transport compatibility: diskless full sync large dataset with replcompression} {
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

    test {Transport compatibility: replcompression replica syncs with uncompressed primary} {
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

    test {Transport compatibility: disk-backed full sync still accepts negotiated transport compression} {
        $primary flushall
        for {set i 0} {$i < 100} {incr i} {
            $primary set "diskbacked:$i" [string repeat "value$i " 20]
        }

        set transfer_count_before [count_message_lines $primary_log "Background RDB transfer started by pid"]
        set compression_count_before [count_message_lines $primary_log "with LZ4 transport compression"]
        set stream_compression_before [count_message_lines $primary_log "Replication compression enabled for replica"]

        start_server {overrides {save "" replcompression yes repl-diskless-load disabled}} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch when disk-backed full sync should stay uncompressed"
            }

            wait_for_condition 50 100 {
                [count_message_lines $primary_log "Background RDB transfer started by pid"] == [expr {$transfer_count_before + 1}]
            } else {
                fail "Primary did not start exactly one RDB transfer for the disk-backed replica"
            }

            assert_equal [expr {$compression_count_before + 1}] [count_message_lines $primary_log "with LZ4 transport compression"]
            wait_for_condition 50 100 {
                [count_message_lines $primary_log "Replication compression enabled for replica"] == [expr {$stream_compression_before + 1}]
            } else {
                fail "Primary did not enable compressed incremental replication after disk-backed full sync"
            }
            assert_equal [string repeat "value42 " 20] [$replica get diskbacked:42]
            $primary set diskbacked-post-sync diskbacked-ok
            wait_for_condition 50 100 {
                [$replica get diskbacked-post-sync] eq "diskbacked-ok"
            } else {
                fail "Replica did not receive post-sync write after disk-backed full sync"
            }

            $replica replicaof no one
        }
    }

    test {Transport compatibility: replica toggling replcompression off after handshake still accepts negotiated compressed full sync} {
        $primary flushall
        for {set i 0} {$i < 150} {incr i} {
            $primary set "toggle:$i" [string repeat "value$i " 25]
        }
        $primary config set repl-diskless-sync-delay 2

        set transfer_count_before [count_message_lines $primary_log "Background RDB transfer started by pid"]
        set compression_count_before [count_message_lines $primary_log "with LZ4 transport compression"]

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
            set replica [srv 0 client]
            set replica_log [srv 0 stdout]
            set signature_errors_before [count_message_lines $replica_log "Wrong signature trying to load DB from file"]

            $replica replicaof $primary_host $primary_port

            wait_for_condition 50 100 {
                [string match "*state=wait_bgsave*" [$primary info replication]]
            } else {
                fail "Replica did not enter wait_bgsave before toggling replcompression"
            }

            $replica config set replcompression no

            wait_for_condition 80 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after replcompression toggle during delayed full sync"
            }

            wait_for_condition 50 100 {
                [count_message_lines $primary_log "Background RDB transfer started by pid"] == [expr {$transfer_count_before + 1}] &&
                [count_message_lines $primary_log "with LZ4 transport compression"] == [expr {$compression_count_before + 1}]
            } else {
                fail "Primary retried full sync instead of succeeding with the negotiated compressed transfer"
            }

            assert_equal $signature_errors_before [count_message_lines $replica_log "Wrong signature trying to load DB from file"]

            $primary set toggle-post-sync "toggle-ok"
            wait_for_condition 50 100 {
                [$replica get toggle-post-sync] eq "toggle-ok"
            } else {
                fail "Replica did not receive post-sync write after replcompression toggle test"
            }

            $replica replicaof no one
        }

        $primary config set repl-diskless-sync-delay 0
    }
    test {Transport compatibility: mixed full sync batch falls back to uncompressed transport when one replica lacks capability} {
        $primary flushall
        for {set i 0} {$i < 200} {incr i} {
            $primary set "mixed:$i" [string repeat "value$i " 30]
        }
        $primary config set repl-diskless-sync-delay 2

        set transfer_count_before [count_message_lines $primary_log "Background RDB transfer started by pid"]
        set compression_count_before [count_message_lines $primary_log "with LZ4 transport compression"]

        start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
            set compressed_replica [srv 0 client]
            start_server {overrides {save "" replcompression no repl-diskless-load swapdb}} {
                set uncompressed_replica [srv 0 client]

                $compressed_replica replicaof $primary_host $primary_port
                $uncompressed_replica replicaof $primary_host $primary_port

                wait_for_condition 80 100 {
                    [status $compressed_replica master_link_status] eq "up" &&
                    [status $uncompressed_replica master_link_status] eq "up" &&
                    [$primary debug digest] eq [$compressed_replica debug digest] &&
                    [$primary debug digest] eq [$uncompressed_replica debug digest]
                } else {
                    fail "Replica digest mismatch after mixed-capability full sync batch"
                }

                wait_for_condition 50 100 {
                    [count_message_lines $primary_log "Background RDB transfer started by pid"] == [expr {$transfer_count_before + 1}]
                } else {
                    fail "Primary did not start exactly one RDB transfer for the mixed-capability batch"
                }

                assert_equal $compression_count_before [count_message_lines $primary_log "with LZ4 transport compression"]

                $primary set mixed-post-sync "mixed-batch"
                wait_for_condition 50 100 {
                    [$compressed_replica get mixed-post-sync] eq "mixed-batch" &&
                    [$uncompressed_replica get mixed-post-sync] eq "mixed-batch"
                } else {
                    fail "Replicas did not receive post-sync write after mixed-capability full sync batch"
                }

                $uncompressed_replica replicaof no one
            }
            $compressed_replica replicaof no one
        }

        $primary config set repl-diskless-sync-delay 0
    }

    test {Transport compatibility: online non-capable replica does not disable transport compression for a capable sync batch} {
        $primary flushall
        for {set i 0} {$i < 150} {incr i} {
            $primary set "scoped:$i" [string repeat "value$i " 25]
        }

        start_server {overrides {save "" replcompression no repl-diskless-load swapdb}} {
            set online_uncompressed_replica [srv 0 client]
            $online_uncompressed_replica replicaof $primary_host $primary_port
            wait_for_sync $online_uncompressed_replica

            wait_for_condition 50 100 {
                [status $online_uncompressed_replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$online_uncompressed_replica debug digest]
            } else {
                fail "Uncompressed replica digest mismatch before scoped transport-compression test"
            }

            # Wait for the first transfer log to appear before capturing baseline counts.
            wait_for_condition 50 100 {
                [count_message_lines $primary_log "Background RDB transfer terminated with success"] > 0
            } else {
                fail "First RDB transfer did not complete"
            }

            set transfer_count_before [count_message_lines $primary_log "Background RDB transfer started by pid"]
            set compression_count_before [count_message_lines $primary_log "with LZ4 transport compression"]

            start_server {overrides {save "" replcompression yes repl-diskless-load swapdb}} {
                set compressed_replica [srv 0 client]
                $compressed_replica replicaof $primary_host $primary_port
                wait_for_sync $compressed_replica

                wait_for_condition 50 100 {
                    [status $compressed_replica master_link_status] eq "up" &&
                    [$primary debug digest] eq [$compressed_replica debug digest]
                } else {
                    fail "Compressed replica digest mismatch when syncing alongside an online non-capable replica"
                }

                wait_for_condition 50 100 {
                    [count_message_lines $primary_log "Background RDB transfer started by pid"] == [expr {$transfer_count_before + 1}] &&
                    [count_message_lines $primary_log "with LZ4 transport compression"] == [expr {$compression_count_before + 1}]
                } else {
                    fail "Primary did not keep transport compression enabled for the capable sync batch"
                }

                $primary set scoped-post-sync "scoped-batch"
                wait_for_condition 50 100 {
                    [$compressed_replica get scoped-post-sync] eq "scoped-batch" &&
                    [$online_uncompressed_replica get scoped-post-sync] eq "scoped-batch"
                } else {
                    fail "Replicas did not receive post-sync write after scoped transport-compression test"
                }

                $compressed_replica replicaof no one
            }

            $online_uncompressed_replica replicaof no one
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

}
