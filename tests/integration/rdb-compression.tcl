tags {"rdb-compression external:skip needs:debug"} {

start_server {overrides {save "" enable-debug-command local}} {
    test {RDB save and load round-trip with LZ4 compression} {
        r config set rdb-compression-algo lz4
        # Populate various data types
        for {set i 0} {$i < 100} {incr i} {
            r set "key:$i" [string repeat "value$i " 100]
        }
        r set counter 42
        r lpush mylist a b c d e
        r sadd myset x y z
        r zadd zset 1.0 a 2.0 b 3.0 c
        r hset myhash f1 v1 f2 v2

        set digest [debug_digest]
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}

        # Spot-check values
        assert_equal 42 [r get counter]
        assert_equal 5 [r llen mylist]
        assert_equal 3 [r scard myset]
        assert_equal 3 [r zcard zset]
        assert_equal "v1" [r hget myhash f1]
    }

    test {RDB save with LZF (default) round-trips correctly} {
        r config set rdb-compression-algo lzf
        r flushall
        for {set i 0} {$i < 50} {incr i} {
            r set "lzf:$i" [string repeat "data$i " 50]
        }

        set digest [debug_digest]
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
    }

    test {Uncompressed RDB files load correctly (backward compat)} {
        r config set rdbcompression no
        r config set rdb-compression-algo lzf
        r flushall
        r set hello world
        r set num 12345

        set digest [debug_digest]
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        r config set rdbcompression yes
    }

    test {LZ4 compressed RDB with large dataset} {
        r config set rdb-compression-algo lz4
        r flushall
        r debug populate 1000
        r set extra_key "extra_value"

        set digest [debug_digest]
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        assert_equal "extra_value" [r get extra_key]
        assert_equal 1001 [r dbsize]
    }

    test {Switching from LZ4 to LZF preserves data} {
        r config set rdb-compression-algo lz4
        r flushall
        r debug populate 100
        set digest [debug_digest]

        # Save with LZ4, then switch to LZF and reload
        r bgsave
        waitForBgsave r
        r config set rdb-compression-algo lzf
        r debug reload nosave
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
    }

    test {Invalid compression algo config is rejected} {
        catch {r config set rdb-compression-algo snappy} err
        assert_match "*argument(s) must be one of the following: lzf, lz4*" $err
    }

    test {Invalid streaming compression level config is rejected} {
        catch {r config set rdb-streaming-compression-level -1001} err
        assert_match "*between* -1000 *22*" $err
        catch {r config set rdb-streaming-compression-level 23} err
        assert_match "*between* -1000 *22*" $err
        catch {r config set rdb-streaming-compression-level not-an-int} err
        assert_match "*parsed into an integer*" $err
    }

    test {Streaming compression level requires LZ4 algo} {
        r config set rdb-streaming-compression-level -5 rdb-compression-algo lzf

        catch {r config set rdb-streaming-compression-level -9} err
        assert_match "*supported only when rdb-compression-algo is lz4*" $err
        assert_equal "lzf" [lindex [r config get rdb-compression-algo] 1]
        assert_equal "-5" [lindex [r config get rdb-streaming-compression-level] 1]

        # Invalid paired updates should fail atomically regardless of argument order.
        catch {r config set rdb-compression-algo lzf rdb-streaming-compression-level -9} err
        assert_match "*supported only when rdb-compression-algo is lz4*" $err
        assert_equal "lzf" [lindex [r config get rdb-compression-algo] 1]
        assert_equal "-5" [lindex [r config get rdb-streaming-compression-level] 1]

        catch {r config set rdb-streaming-compression-level -9 rdb-compression-algo lzf} err
        assert_match "*supported only when rdb-compression-algo is lz4*" $err
        assert_equal "lzf" [lindex [r config get rdb-compression-algo] 1]
        assert_equal "-5" [lindex [r config get rdb-streaming-compression-level] 1]

        r config set rdb-compression-algo lz4
        r config set rdb-streaming-compression-level -9
        catch {r config set rdb-compression-algo lzf} err
        assert_match "*supported only when rdb-compression-algo is lz4*" $err
        assert_equal "lz4" [lindex [r config get rdb-compression-algo] 1]
        assert_equal "-9" [lindex [r config get rdb-streaming-compression-level] 1]

        catch {r config set rdb-compression-algo lzf rdb-streaming-compression-level -9} err
        assert_match "*supported only when rdb-compression-algo is lz4*" $err
        assert_equal "lz4" [lindex [r config get rdb-compression-algo] 1]
        assert_equal "-9" [lindex [r config get rdb-streaming-compression-level] 1]

        r config set rdb-streaming-compression-level -5 rdb-compression-algo lzf
        assert_equal "lzf" [lindex [r config get rdb-compression-algo] 1]
        assert_equal "-5" [lindex [r config get rdb-streaming-compression-level] 1]

        # Restore LZ4 so following tests keep their existing assumptions.
        r config set rdb-compression-algo lz4
    }

    test {Startup rejects non-LZ4 with non-default streaming level} {
        set confdir [tmpdir "rdb-compression-invalid-startup"]
        exec mkdir -p $confdir
        set cfgfile [file join $confdir "valkey.conf"]
        set pidfile [file join $confdir "startup.pid"]
        set port [find_available_port $::baseport $::portcount]

        set fd [open $cfgfile w]
        puts $fd "port $port"
        puts $fd "bind 127.0.0.1"
        puts $fd "save \"\""
        puts $fd "dir $confdir"
        puts $fd "daemonize yes"
        puts $fd "pidfile $pidfile"
        puts $fd "logfile /dev/null"
        puts $fd "rdb-compression-algo lzf"
        puts $fd "rdb-streaming-compression-level -9"
        close $fd

        set rc [catch {exec $::VALKEY_SERVER_BIN $cfgfile} err]
        assert {$rc == 1}
        assert_match "*rdb-streaming-compression-level is supported only when rdb-compression-algo is lz4*" $err

        # Defensive cleanup in case startup unexpectedly succeeded.
        if {[file exists $pidfile]} {
            set pf [open $pidfile r]
            set pid [string trim [read $pf]]
            close $pf
            if {$pid ne ""} {
                catch {exec kill $pid}
                catch {exec kill -9 $pid}
            }
        }
    }

    test {RDB compression configs survive CONFIG REWRITE and restart} {
        r config set rdbcompression yes
        r config set rdb-compression-algo lz4
        r config set rdb-streaming-compression-level -9
        r config rewrite

        restart_server 0 true false

        assert_equal "yes" [lindex [r config get rdbcompression] 1]
        assert_equal "lz4" [lindex [r config get rdb-compression-algo] 1]
        assert_equal "-9" [lindex [r config get rdb-streaming-compression-level] 1]
    }

    test {LZ4 compressed RDB with rdb-checksum yes uses codec checksum} {
        r config set rdb-compression-algo lz4
        r flushall
        for {set i 0} {$i < 200} {incr i} {
            r set "cksum:$i" [string repeat "payload$i " 200]
        }

        set digest [debug_digest]
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
    }

    test {LZ4 compressed RDB detects corruption in compressed payload} {
        r config set rdb-compression-algo lz4
        r flushall
        for {set i 0} {$i < 100} {incr i} {
            r set "corrupt:$i" [string repeat "testdata$i " 100]
        }

        # Save to file
        r bgsave
        waitForBgsave r

        set rdbfile [file join [lindex [r config get dir] 1] dump.rdb]

        # Read the file, flip a byte in the compressed payload
        # (skip the VKCS envelope at offset 0-7, corrupt somewhere in the middle)
        set fd [open $rdbfile r+]
        fconfigure $fd -translation binary
        set data [read $fd]
        set len [string length $data]
        # Corrupt a byte roughly in the middle of the compressed data
        set pos [expr {$len / 2}]
        set byte [string index $data $pos]
        binary scan $byte c val
        set newval [expr {($val + 1) & 0xFF}]
        set newbyte [binary format c $newval]
        set data [string replace $data $pos $pos $newbyte]
        seek $fd 0
        puts -nonewline $fd $data
        close $fd

        # Reload should fail due to corruption
        catch {r debug reload nosave} err
        assert_match "*Error*" $err
    }

    test {Invalid non-VKCS/non-RDB file fails reload} {
        r config set rdb-compression-algo lz4
        r flushall
        r set smoke-key smoke-value

        r bgsave
        waitForBgsave r

        set rdbfile [file join [lindex [r config get dir] 1] dump.rdb]
        set fd [open $rdbfile w]
        fconfigure $fd -translation binary
        puts -nonewline $fd "NOTANRDB"
        close $fd

        catch {r debug reload nosave} err
        assert_match "*Error*" $err
    }
}

start_server {overrides {save "" enable-debug-command local rdbchecksum no}} {
    test {LZ4 compressed RDB with rdb-checksum no loads correctly} {
        r config set rdb-compression-algo lz4
        r flushall
        for {set i 0} {$i < 50} {incr i} {
            r set "nocksum:$i" [string repeat "data$i " 100]
        }

        set digest [debug_digest]
        r debug reload
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
    }
}

start_server {tags {"rdb-compression repl external:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]

    start_server {overrides {save "" enable-debug-command local}} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        test {Full sync replication works with LZ4-compressed RDB snapshot} {
            $primary config set rdbcompression yes
            $primary config set rdb-compression-algo lz4
            $primary config set rdb-streaming-compression-level -5
            $primary flushall
            for {set i 0} {$i < 500} {incr i} {
                $primary set "repl:$i" [string repeat "payload$i " 40]
            }

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after LZ4 RDB full sync"
            }

            assert_equal [string repeat "payload42 " 40] [$replica get repl:42]
        }

        test {Replication continues after LZ4 full sync} {
            $primary set repl:post-sync "after-sync"
            wait_for_condition 50 100 {
                [$replica get repl:post-sync] eq "after-sync"
            } else {
                fail "Replica did not receive post-sync write"
            }
        }

        test {Diskless full sync replication works with LZ4-compressed RDB snapshot} {
            $replica replicaof no one
            $primary config set repl-diskless-sync yes
            $primary config set repl-diskless-sync-delay 0
            $replica config set repl-diskless-load swapdb
            $primary flushall
            for {set i 0} {$i < 300} {incr i} {
                $primary set "diskless:$i" [string repeat "payload$i " 60]
            }

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after diskless LZ4 full sync"
            }

            assert_equal [string repeat "payload42 " 60] [$replica get diskless:42]
            $primary config set repl-diskless-sync no
        }

        $replica replicaof no one
    }
}

set cluster_bus_port [find_available_port $::baseport $::portcount]
start_server [list tags {"rdb-compression cluster external:skip singledb"} overrides [list save "" cluster-enabled yes cluster-port $cluster_bus_port]] {
    test {RDB compression configs validate in cluster mode} {
        catch {r config set rdb-streaming-compression-level -9} err
        assert_match "*supported only when rdb-compression-algo is lz4*" $err

        r config set rdb-compression-algo lz4
        r config set rdb-streaming-compression-level -5
        assert_equal "OK" [r save]
    }
}

}
