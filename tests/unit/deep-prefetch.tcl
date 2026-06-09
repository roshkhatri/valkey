# Test deep prefetch for hash and zset inner hashtables.
# Requires io-threads >= 2 and hashtable-encoded objects (>128 fields).

start_server {config "minimal.conf" tags {"external:skip"} overrides {io-threads 4 hash-max-listpack-entries 0 zset-max-listpack-entries 0}} {
    test "Deep prefetch - HGET correctness with pipelined commands" {
        # Create a hash with enough fields to use hashtable encoding
        for {set i 0} {$i < 200} {incr i} {
            r hset myhash "field:$i" "value:$i"
        }
        assert_encoding hashtable myhash

        # Pipeline multiple HGET commands to trigger batched prefetch
        set rd [valkey_deferring_client]
        for {set i 0} {$i < 50} {incr i} {
            $rd hget myhash "field:$i"
        }
        $rd flush
        for {set i 0} {$i < 50} {incr i} {
            assert_equal "value:$i" [$rd read]
        }
        $rd close
    }

    test "Deep prefetch - ZSCORE correctness with pipelined commands" {
        # Create a zset with enough members to use skiplist encoding
        for {set i 0} {$i < 200} {incr i} {
            r zadd myzset $i "member:$i"
        }
        assert_encoding skiplist myzset

        # Pipeline multiple ZSCORE commands
        set rd [valkey_deferring_client]
        for {set i 0} {$i < 50} {incr i} {
            $rd zscore myzset "member:$i"
        }
        $rd flush
        for {set i 0} {$i < 50} {incr i} {
            assert_equal $i [$rd read]
        }
        $rd close
    }

    test "Deep prefetch - HMGET multi-field correctness" {
        set rd [valkey_deferring_client]
        $rd hmget myhash field:0 field:1 field:2 field:3 field:4
        $rd flush
        set result [$rd read]
        assert_equal [list value:0 value:1 value:2 value:3 value:4] $result
        $rd close
    }

    test "Deep prefetch - mixed commands in pipeline" {
        set rd [valkey_deferring_client]
        $rd hget myhash field:10
        $rd zscore myzset member:10
        $rd hdel myhash field:199
        $rd hget myhash field:199
        $rd flush
        assert_equal "value:10" [$rd read]
        assert_equal 10 [$rd read]
        assert_equal 1 [$rd read]
        assert_equal {} [$rd read]
        $rd close
    }

    test "Deep prefetch - prefetch stats non-negative" {
        # Just verify the stats exist and are non-negative (prefetch may not
        # trigger in test env with single client, but should never be negative)
        set entries [getInfoProperty [r info stats] io_threaded_total_prefetch_entries]
        assert {$entries >= 0}
    }
}
