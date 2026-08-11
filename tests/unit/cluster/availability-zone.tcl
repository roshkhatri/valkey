proc read_file {path} {
    set fd [open $path r]
    set data [read $fd]
    close $fd
    return $data
}

proc file_has_pattern {path pattern} {
    if {![file exists $path]} {
        return 0
    }
    return [regexp $pattern [read_file $path]]
}

proc cluster_nodes_conf_path {id} {
    set dir [lindex [R $id config get dir] 1]
    set conf [lindex [R $id config get cluster-config-file] 1]
    return [file join $dir $conf]
}

proc cluster_shards_node {observer node_id} {
    foreach shard [R $observer CLUSTER SHARDS] {
        foreach node [dict get $shard nodes] {
            if {[dict get $node id] eq $node_id} {
                return $node
            }
        }
    }
    return {}
}

start_cluster 2 0 {tags {external:skip cluster} overrides {cluster-ping-interval 100}} {
    test "Availability zone appears in SLOTS/SHARDS" {
        set slots_resp [R 0 CLUSTER SLOTS]
        set slots_str [join $slots_resp " "]

        assert_no_match "*zone-a*" $slots_str
        assert_no_match "*zone-b*" $slots_str
        assert_no_match "*zone-c*" $slots_str

        set shards_resp [R 0 CLUSTER SHARDS]
        set shards_str [join $shards_resp " "]

        assert_no_match "*zone-a*" $shards_str
        assert_no_match "*zone-b*" $shards_str
        assert_no_match "*zone-c*" $shards_str

        # Empty AZ -> Non Empty AZ
        R 0 CONFIG SET availability-zone zone-a
        R 1 CONFIG SET availability-zone zone-b

        wait_for_condition 50 100 {
            [string match "*zone-a*" [join [R 0 CLUSTER SLOTS] " "]] &&
            [string match "*zone-b*" [join [R 0 CLUSTER SLOTS] " "]]
        } else {
            fail "Availability zone was not propagated in CLUSTER SLOTS"
        }

        set slots_resp [R 0 CLUSTER SLOTS]
        set slots_str [join $slots_resp " "]
        assert_match "*availability-zone*" $slots_str
        assert_match "*zone-a*" $slots_str
        assert_match "*zone-b*" $slots_str

        wait_for_condition 50 100 {
            [string match "*zone-a*" [join [R 0 CLUSTER SHARDS] " "]] &&
            [string match "*zone-b*" [join [R 0 CLUSTER SHARDS] " "]]
        } else {
            fail "Availability zone was not propagated in CLUSTER SHARDS"
        }

        set shards_resp [R 0 CLUSTER SHARDS]
        set shards_str [join $shards_resp " "]
        assert_match "*availability-zone*" $shards_str
        assert_match "*zone-a*" $shards_str
        assert_match "*zone-b*" $shards_str

        # Non Empty AZ -> Non Empty AZ
        R 0 CONFIG SET availability-zone zone-c

        wait_for_condition 50 100 {
            [string match "*zone-c*" [join [R 1 CLUSTER SLOTS] " "]]
        } else {
            fail "Availability zone was not propagated in CLUSTER SLOTS"
        }

        set slots_resp [R 1 CLUSTER SLOTS]
        set slots_str [join $slots_resp " "]
        assert_match "*availability-zone*" $slots_str
        assert_match "*zone-c*" $slots_str

        wait_for_condition 50 100 {
            [string match "*zone-c*" [join [R 1 CLUSTER SHARDS] " "]]
        } else {
            fail "Availability zone was not propagated in CLUSTER SHARDS"
        }

        set shards_resp [R 1 CLUSTER SHARDS]
        set shards_str [join $shards_resp " "]
        assert_match "*availability-zone*" $shards_str
        assert_match "*zone-c*" $shards_str
    }

    test "Availability zone removed when set to empty string" {
        R 0 CONFIG SET availability-zone ""
        R 1 CONFIG SET availability-zone ""

        wait_for_condition 50 100 {
            ![string match "*availability-zone*" [join [R 0 CLUSTER SLOTS] " "]] &&
            ![string match "*availability-zone*" [join [R 0 CLUSTER SHARDS] " "]]
        } else {
            fail "Availability zone was not cleared from CLUSTER SLOTS/SHARDS"
        }

        set slots_resp [R 0 CLUSTER SLOTS]
        set slots_str [join $slots_resp " "]
        assert_no_match "*availability-zone*" $slots_str
        assert_no_match "*zone-a*" $slots_str
        assert_no_match "*zone-b*" $slots_str
        assert_no_match "*zone-c*" $slots_str

        set shards_resp [R 0 CLUSTER SHARDS]
        set shards_str [join $shards_resp " "]
        assert_no_match "*availability-zone*" $shards_str
    }

    test "Oversized availability zone is omitted from PING extensions" {
        set retained_az retained-before-oversized
        assert_equal OK [R 0 CONFIG SET availability-zone $retained_az]

        wait_for_condition 50 100 {
            [string match "*$retained_az*" [join [R 1 CLUSTER SHARDS] " "]]
        } else {
            fail "Availability zone was not propagated before setting an oversized value"
        }

        set loglines [count_log_lines 0]
        set oversized_az [string repeat z [expr {16 * 1024 * 1024}]]
        assert_equal OK [R 0 CONFIG SET availability-zone $oversized_az]
        unset oversized_az

        wait_for_log_messages 0 [list \
            "*Omitting lower-priority extensions from * packet*selected*within the remaining packet budget*"] \
            $loglines 500 10

        set source_id [R 0 CLUSTER MYID]
        set announced_ip 192.0.2.10
        set announced_port 6399
        assert_equal OK [R 0 CONFIG SET cluster-announce-client-ipv4 $announced_ip]
        assert_equal OK [R 0 CONFIG SET cluster-announce-client-port $announced_port]
        assert_equal OK [R 0 CONFIG SET cluster-announce-client-tls-port $announced_port]
        wait_for_condition 50 100 {
            [dict get [cluster_shards_node 1 $source_id] ip] eq $announced_ip &&
            [dict get [cluster_shards_node 1 $source_id] port] eq $announced_port &&
            (!$::tls || [dict get [cluster_shards_node 1 $source_id] tls-port] eq $announced_port)
        } else {
            fail "Endpoint metadata was not propagated with an oversized availability-zone extension"
        }

        assert_equal PONG [R 0 PING]
        assert_equal PONG [R 1 PING]
        wait_for_cluster_state ok
        assert_match "*$retained_az*" [join [R 1 CLUSTER SHARDS] " "]

        assert_equal OK [R 0 CONFIG SET availability-zone ""]
        assert_equal OK [R 0 CONFIG SET cluster-announce-client-ipv4 ""]
        assert_equal OK [R 0 CONFIG SET cluster-announce-client-port 0]
        assert_equal OK [R 0 CONFIG SET cluster-announce-client-tls-port 0]
        wait_for_cluster_propagation
    }

    test "Load cluster az config on server start" {
        R 0 config set availability-zone load-az0
        R 1 config set availability-zone load-az1
        R 0 config rewrite
        R 1 config rewrite

        set nodes_conf0 [cluster_nodes_conf_path 0]
        set nodes_conf1 [cluster_nodes_conf_path 1]
        wait_for_condition 50 100 {
            [file exists $nodes_conf0] &&
            [file exists $nodes_conf1] &&
            [file_has_pattern $nodes_conf0 {availability-zone=load-az0}] &&
            [file_has_pattern $nodes_conf0 {availability-zone=load-az1}] &&
            [file_has_pattern $nodes_conf1 {availability-zone=load-az0}] &&
            [file_has_pattern $nodes_conf1 {availability-zone=load-az1}]
        } else {
            fail "Availability zone was not persisted to nodes.conf"
        }

        restart_server 0 true false
        wait_for_cluster_propagation

        wait_for_condition 50 100 {
            [string match "*load-az0*" [join [R 0 CLUSTER SLOTS] " "]] &&
            [string match "*load-az1*" [join [R 0 CLUSTER SLOTS] " "]]
        } else {
            fail "Availability zone was not restored after restart in CLUSTER SLOTS"
        }

        wait_for_condition 50 100 {
            [string match "*load-az0*" [join [R 0 CLUSTER SHARDS] " "]] &&
            [string match "*load-az1*" [join [R 0 CLUSTER SHARDS] " "]]
        } else {
            fail "Availability zone was not restored after restart in CLUSTER SHARDS"
        }
    }
}


start_cluster 1 2 {
    tags {external:skip cluster}
    overrides {cluster-ping-interval 100 cluster-replica-no-failover yes}
} {
    test "Forgotten-node state is prioritized over oversized descriptive extensions" {
        set loglines [count_log_lines 0]
        set oversized_az [string repeat z [expr {16 * 1024 * 1024}]]
        assert_equal OK [R 0 CONFIG SET availability-zone $oversized_az]
        unset oversized_az
        wait_for_log_messages 0 [list \
            "*Omitting lower-priority extensions from * packet*selected*within the remaining packet budget*"] \
            $loglines 500 10

        isolate_node 2
        assert_equal OK [R 0 CONFIG SET availability-zone ""]
    }
}
