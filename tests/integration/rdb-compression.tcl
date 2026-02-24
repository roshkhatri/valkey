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

}
