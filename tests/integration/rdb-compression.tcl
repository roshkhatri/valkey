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

    test {ZSTD config is rejected} {
        catch {r config set rdb-compression-algo zstd} err
        assert_match "*ZSTD compression is not yet supported*" $err
    }
}

}
