# Test PUBLISH propagation across the cluster.

start_cluster 5 5 {tags {external:skip cluster}} {

proc test_cluster_publish {instance instances} {
    # Subscribe all the instances but the one we use to send.
    for {set j 0} {$j < $instances} {incr j} {
        if {$j != $instance} {
            R $j deferred 1
            R $j subscribe testchannel
            R $j read; # Read the subscribe reply
        }
    }

    set data [randomValue]
    R $instance PUBLISH testchannel $data

    # Read the message back from all the nodes.
    for {set j 0} {$j < $instances} {incr j} {
        if {$j != $instance} {
            set msg [R $j read]
            assert {$data eq [lindex $msg 2]}
            R $j unsubscribe testchannel
            R $j read; # Read the unsubscribe reply
            R $j deferred 0
        }
    }
}

test "Test publishing to master" {
    test_cluster_publish 0 10
}

test "Test publishing to slave" {
    test_cluster_publish 5 10
}
} ;# start_cluster

start_cluster 3 0 {tags {external:skip cluster}} {
    test "Test cluster info stats for publish" {
        R 0 CONFIG RESETSTAT
        R 1 CONFIG RESETSTAT
        R 2 CONFIG RESETSTAT

        R 0 PUBLISH hello world
        assert_equal 2 [CI 0 cluster_stats_messages_publish_sent]
        wait_for_condition 50 100 {
            [CI 1 cluster_stats_messages_publish_received] eq 1 &&
            [CI 2 cluster_stats_messages_publish_received] eq 1 &&
            [CI 1 cluster_stats_pubsub_bytes_received] > 0 &&
            [CI 2 cluster_stats_pubsub_bytes_received] > 0
        } else {
            fail "node 2 or node 3 didn't receive clusterbus publish packet"
        }

        set sender_pubsub_bytes [CI 0 cluster_stats_pubsub_bytes_sent]
        set receiver_pubsub_bytes [expr {[CI 1 cluster_stats_pubsub_bytes_received] + [CI 2 cluster_stats_pubsub_bytes_received]}]
        assert_morethan $sender_pubsub_bytes 0
        assert_equal $sender_pubsub_bytes $receiver_pubsub_bytes
    }

    test "PUBLISH accepts the exact cluster packet limit" {
        set max_combined_payload [expr {16 * 1024 * 1024 - 2256 - 8}]
        set channel exact-limit
        set exact [string repeat x [expr {$max_combined_payload - [string length $channel]}]]

        assert_equal 0 [R 0 PUBLISH $channel $exact]
        assert_error {*Message is too large to propagate in cluster mode*} {
            R 0 PUBLISH $channel "${exact}x"
        }

        unset exact
    }

    test "Oversized PUBLISH is rejected before local delivery" {
        set subscriber [valkey_deferring_client 0]
        assert_equal {1} [subscribe $subscriber {oversized-channel}]
        set oversized [string repeat x [expr {16 * 1024 * 1024}]]

        assert_error {*Message is too large to propagate in cluster mode*} {
            R 0 PUBLISH oversized-channel $oversized
        }
        assert_equal 1 [R 0 PUBLISH oversized-channel marker]
        set delivered [$subscriber read]
        assert_equal 6 [string length [lindex $delivered 2]]
        assert_equal {message oversized-channel marker} $delivered

        $subscriber close
        unset oversized
    }
}
