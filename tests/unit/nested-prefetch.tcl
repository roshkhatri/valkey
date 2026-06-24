# Test nested prefetch for hash and zset inner hashtables.
# Requires io-threads >= 2 and hashtable-encoded objects (>128 fields).

start_server {config "minimal.conf" tags {"external:skip"} overrides {io-threads 4 io-threads-always-active yes hash-max-listpack-entries 0 zset-max-listpack-entries 0}} {
    test "Nested prefetch - HGET correctness with pipelined commands" {
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

    test "Nested prefetch - ZSCORE correctness with pipelined commands" {
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

    test "Nested prefetch - HMGET multi-field correctness" {
        set rd [valkey_deferring_client]
        $rd hmget myhash field:0 field:1 field:2 field:3 field:4
        $rd flush
        set result [$rd read]
        assert_equal [list value:0 value:1 value:2 value:3 value:4] $result
        $rd close
    }

    test "Nested prefetch - mixed commands in pipeline" {
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

    test "Nested prefetch - stats increment after batched hash/zset lookups" {
        set before [getInfoProperty [r info stats] io_threaded_total_prefetch_entries]

        # Drive a batch of concurrent pipelined lookups across multiple clients
        # so the prefetch batch fills and the nested-prefetch path executes.
        set clients {}
        for {set c 0} {$c < 8} {incr c} {
            set rd [valkey_deferring_client]
            lappend clients $rd
            for {set i 0} {$i < 100} {incr i} {
                $rd hget myhash "field:[expr {$i % 200}]"
            }
            $rd flush
        }
        foreach rd $clients {
            for {set i 0} {$i < 100} {incr i} { $rd read }
            $rd close
        }

        # With io-threads active and batched hashtable lookups, the prefetch
        # path must have processed entries.
        wait_for_condition 50 100 {
            [getInfoProperty [r info stats] io_threaded_total_prefetch_entries] > $before
        } else {
            fail "io_threaded_total_prefetch_entries did not increase after batched lookups"
        }
    }

    test "Nested prefetch - large (non-embedded) hash values exercise value phase" {
        # Values >128 bytes are stored as a separate allocation (non-embedded),
        # which triggers the optional NESTED_PREFETCH_VALUE phase. Verify both
        # correctness and that the prefetch path runs for these values.
        set big [string repeat "x" 512]
        for {set i 0} {$i < 200} {incr i} {
            r hset bighash "field:$i" "$big:$i"
        }
        assert_encoding hashtable bighash

        set before [getInfoProperty [r info stats] io_threaded_total_prefetch_entries]
        set clients {}
        for {set c 0} {$c < 8} {incr c} {
            set rd [valkey_deferring_client]
            lappend clients $rd
            for {set i 0} {$i < 100} {incr i} {
                $rd hget bighash "field:[expr {$i % 200}]"
            }
            $rd flush
        }
        foreach rd $clients {
            for {set i 0} {$i < 100} {incr i} {
                assert_equal "$big:$i" [$rd read]
            }
            $rd close
        }

        wait_for_condition 50 100 {
            [getInfoProperty [r info stats] io_threaded_total_prefetch_entries] > $before
        } else {
            fail "prefetch entries did not increase for non-embedded hash values"
        }
    }
}
