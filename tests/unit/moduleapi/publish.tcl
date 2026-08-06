set testmodule [file normalize tests/modules/publish.so]

start_server {tags {"modules"}} {
    r module load $testmodule

    test {PUBLISH and SPUBLISH via a module} {
        set rd1 [valkey_deferring_client]
        set rd2 [valkey_deferring_client]

        assert_equal {1} [ssubscribe $rd1 {chan1}]
        assert_equal {1} [subscribe $rd2 {chan1}]
        assert_equal 1 [r publish.shard chan1 hello]
        assert_equal 1 [r publish.classic chan1 world]
        assert_equal {smessage chan1 hello} [$rd1 read]
        assert_equal {message chan1 world} [$rd2 read]
        $rd1 close
        $rd2 close
    }

    test {module publish to self with multi message} {
        r hello 3
        r subscribe foo

        # published message comes after the response of the command that issued it.
        assert_equal [r publish.classic_multi foo bar vaz] {1 1}
        assert_equal [r read] {message foo bar}
        assert_equal [r read] {message foo vaz}

        r unsubscribe foo
        r hello 2
        set _ ""
    } {} {resp3}

}

start_cluster 1 0 [list tags {external:skip cluster modules} config_lines [list loadmodule $testmodule]] {
    test {module PUBLISH and SPUBLISH accept the exact cluster packet limit} {
        set max_combined_payload [expr {16 * 1024 * 1024 - 2256 - 8}]
        set channel exact-limit
        set exact [string repeat x [expr {$max_combined_payload - [string length $channel]}]]

        assert_equal {0 0} [R 0 publish.classic_status $channel $exact]
        assert_equal {-1 1} [R 0 publish.classic_status $channel "${exact}x"]
        assert_equal {0 0} [R 0 publish.shard_status $channel $exact]
        assert_equal {-1 1} [R 0 publish.shard_status $channel "${exact}x"]

        unset exact
    }

    test {oversized module PUBLISH and SPUBLISH fail before local delivery} {
        set classic_subscriber [valkey_deferring_client 0]
        set shard_subscriber [valkey_deferring_client 0]
        assert_equal {1} [subscribe $classic_subscriber {oversized-classic}]
        assert_equal {1} [ssubscribe $shard_subscriber {oversized-shard}]
        set oversized [string repeat x [expr {16 * 1024 * 1024}]]

        assert_equal {-1 1} [R 0 publish.classic_status oversized-classic $oversized]
        assert_equal 1 [R 0 publish.classic oversized-classic marker]
        set delivered [$classic_subscriber read]
        assert_equal 6 [string length [lindex $delivered 2]]
        assert_equal {message oversized-classic marker} $delivered

        assert_equal {-1 1} [R 0 publish.shard_status oversized-shard $oversized]
        assert_equal 1 [R 0 publish.shard oversized-shard marker]
        set delivered [$shard_subscriber read]
        assert_equal 6 [string length [lindex $delivered 2]]
        assert_equal {smessage oversized-shard marker} $delivered

        $classic_subscriber close
        $shard_subscriber close
        unset oversized
    }
}
