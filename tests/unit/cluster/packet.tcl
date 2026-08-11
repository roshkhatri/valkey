# Create a clusterbus message packet
proc create_cluster_meet_packet {sender_name sender_port sender_cport {count 0} {extensions 0} {mflags 0}} {
    # Constants
    set CLUSTER_NAMELEN 40
    set CLUSTER_SLOTS 16384
    set NET_IP_STR_LEN 46
    set CLUSTERMSG_TYPE_MEET 2
    set CLUSTERMSG_MIN_LEN 2256

    # Build the packet
    set packet ""

    # Signature "RCmb" (4 bytes)
    append packet "RCmb"

    # totlen (uint32_t) - will be updated at the end
    append packet [binary format I 0]

    # ver (uint16_t) - protocol version 1
    append packet [binary format S 1]

    # port (uint16_t)
    append packet [binary format S $sender_port]

    # type (uint16_t) - MEET
    append packet [binary format S $CLUSTERMSG_TYPE_MEET]

    # count (uint16_t)
    append packet [binary format S $count]

    # currentEpoch (uint64_t)
    append packet [binary format W 1]

    # configEpoch (uint64_t)
    append packet [binary format W 1]

    # offset (uint64_t)
    append packet [binary format W 0]

    # sender[40] - node name
    set sender_padded [string range "${sender_name}[string repeat "\x00" $CLUSTER_NAMELEN]" 0 [expr {$CLUSTER_NAMELEN - 1}]]
    append packet $sender_padded

    # myslots[2048] - all zeros
    append packet [string repeat "\x00" [expr {$CLUSTER_SLOTS / 8}]]

    # replicaof[40] - all zeros
    append packet [string repeat "\x00" $CLUSTER_NAMELEN]

    # myip[46] - all zeros
    append packet [string repeat "\x00" $NET_IP_STR_LEN]

    # extensions (uint16_t)
    append packet [binary format S $extensions]

    # notused1[30] - reserved
    append packet [string repeat "\x00" 30]

    # pport (uint16_t)
    append packet [binary format S 0]

    # cport (uint16_t) - cluster bus port
    append packet [binary format S $sender_cport]

    # flags (uint16_t) - CLUSTER_NODE_PRIMARY
    append packet [binary format S 1]

    # state (unsigned char) - CLUSTER_OK
    append packet [binary format c 0]

    # mflags[3] - message flags (WITH CLUSTERMSG_FLAG0_EXT_DATA flag set)
    # CLUSTERMSG_FLAG0_EXT_DATA = (1 << 2) = 4
    append packet [binary format ccc $mflags 0 0]

    # Update totlen
    set totlen [string length $packet]
    set packet [string replace $packet 4 7 [binary format I $totlen]]

    return $packet
}

proc create_cluster_initial_header {totlen type {light 0}} {
    if {$light} {
        set type [expr {$type | 0x8000}]
    }

    set packet "RCmb"
    append packet [binary format I $totlen]
    append packet [binary format S 1]
    append packet [binary format S 0]
    append packet [binary format S $type]
    append packet [binary format S 0]
    return $packet
}

proc create_cluster_meet_packet_with_extensions {
    sender_name sender_port sender_cport extensions mflags extension_data
} {
    set packet [create_cluster_meet_packet \
        $sender_name $sender_port $sender_cport 0 $extensions $mflags]
    append packet $extension_data
    return [string replace $packet 4 7 [binary format I [string length $packet]]]
}

proc send_cluster_packet {cluster_port packet} {
    set sock [socket 127.0.0.1 $cluster_port]
    fconfigure $sock -translation binary -buffering none -blocking 1
    puts -nonewline $sock $packet
    flush $sock
    close $sock
}

proc cluster_bus_socket_is_at_eof {sock} {
    set data [read $sock 1]
    if {[string length $data] || [fblocked $sock]} {
        return 0
    }
    return [eof $sock]
}

proc assert_cluster_bus_initial_header_accepted {cluster_port header} {
    assert_equal 16 [string length $header]

    set sock [socket 127.0.0.1 $cluster_port]
    fconfigure $sock -translation binary -buffering none -blocking 1
    puts -nonewline $sock $header
    flush $sock
    fconfigure $sock -blocking 0

    after 50
    set rejected [cluster_bus_socket_is_at_eof $sock]
    catch {close $sock}
    assert_equal 0 $rejected
}

proc assert_cluster_bus_initial_header_rejected {cluster_port header} {
    assert_equal 16 [string length $header]

    set sock [socket 127.0.0.1 $cluster_port]
    fconfigure $sock -translation binary -buffering none -blocking 1
    puts -nonewline $sock $header
    flush $sock
    fconfigure $sock -blocking 0

    wait_for_condition 100 10 {
        [cluster_bus_socket_is_at_eof $sock]
    } else {
        catch {close $sock}
        fail "Cluster bus connection remained open after an invalid initial header"
    }
    catch {close $sock}
}

start_cluster 1 0 {tags {external:skip cluster tls:skip}} {
    test "Malformed ping extensions are rejected" {
        set base_port [srv 0 port]
        set cluster_port [expr {$base_port + 10000}]
        set fake_node_id "abcdef1234567890abcdef1234567890abcdef12"

        foreach {extensions mflags extension_data expected_log} [list \
            1 4 [binary format ISS 0 99 0] \
                "*Received a meet packet with invalid extension length (0 bytes)*" \
            1 4 [binary format ISS 4 99 0] \
                "*Received a meet packet with invalid extension length (4 bytes)*" \
            1 4 "[binary format ISS 12 99 0]ABCD" \
                "*Received a meet packet with invalid extension length (12 bytes)*" \
            1 4 [binary format ISS 16 99 0] \
                "*Received invalid meet packet with extension data that exceeds total packet length*" \
            1 4 [binary format ISS 8 2 0] \
                "*Received a meet packet with invalid extension data*" \
            1 4 [binary format ISS 8 3 0] \
                "*Received a meet packet with invalid extension data*" \
            1 4 [binary format ISS 8 6 0] \
                "*Received a meet packet with invalid extension data*" \
            1 4 [binary format ISS 8 7 0] \
                "*Received a meet packet with invalid extension data*" \
            1 4 "[binary format ISS 16 0 0]ABCDEFGH" \
                "*Received a meet packet with invalid extension data*" \
            1 4 "[binary format ISS 16 1 0]ABCDEFGH" \
                "*Received a meet packet with invalid extension data*" \
            1 4 "[binary format ISS 16 4 0]ABCDEFGH" \
                "*Received a meet packet with invalid extension data*" \
            1 4 "[binary format ISS 16 5 0]ABCDEFGH" \
                "*Received a meet packet with invalid extension data*" \
            1 4 "[binary format ISS 16 8 0]ABCDEFGH" \
                "*Received a meet packet with invalid extension data*" \
            1 0 "" \
                "*Received invalid meet packet with extension data that exceeds total packet length*"] {
            set loglines [count_log_lines 0]
            set packet [create_cluster_meet_packet_with_extensions \
                $fake_node_id $base_port $cluster_port $extensions $mflags $extension_data]
            send_cluster_packet $cluster_port $packet
            wait_for_log_messages 0 [list $expected_log] $loglines 100 10
            assert_equal PONG [R 0 ping]
        }
    }

    test "Valid ping extension types are accepted" {
        set base_port [srv 0 port]
        set cluster_port [expr {$base_port + 10000}]
        set fake_node_id "abcdef1234567890abcdef1234567890abcdef12"
        set string_payload "value\x00\x00\x00"
        set forgotten_payload "${fake_node_id}[binary format W 60]"
        set port_payload "[binary format S 1234][string repeat \x00 6]"

        foreach extension_data [list \
            "[binary format ISS 16 0 0]${string_payload}" \
            "[binary format ISS 16 1 0]${string_payload}" \
            "[binary format ISS 56 2 0]${forgotten_payload}" \
            "[binary format ISS 48 3 0]${fake_node_id}" \
            "[binary format ISS 16 4 0]${string_payload}" \
            "[binary format ISS 16 5 0]${string_payload}" \
            "[binary format ISS 16 6 0]${port_payload}" \
            "[binary format ISS 16 7 0]${port_payload}" \
            "[binary format ISS 16 8 0]${string_payload}" \
            [binary format ISS 8 99 0]] {
            set packet [create_cluster_meet_packet_with_extensions \
                $fake_node_id $base_port $cluster_port 1 4 $extension_data]
            set packet [string replace $packet 12 13 [binary format S 0]]

            set sock [socket 127.0.0.1 $cluster_port]
            fconfigure $sock -translation binary -buffering none -blocking 1
            puts -nonewline $sock $packet
            flush $sock
            set hdr [read $sock 16]
            assert_equal 16 [string length $hdr]
            binary scan $hdr @4I totlen
            set rest [read $sock [expr {$totlen - 16}]]
            close $sock

            set reply "${hdr}${rest}"
            binary scan $reply @12Su type
            assert_equal 1 $type
        }
        assert_equal PONG [R 0 ping]
    }

    test "Cluster bus total length boundary is enforced after the initial header" {
        set cluster_port [expr {[srv 0 port] + 10000}]
        set max_packet_len [expr {16 * 1024 * 1024}]

        foreach light {0 1} {
            set header [create_cluster_initial_header $max_packet_len 4 $light]
            assert_cluster_bus_initial_header_accepted $cluster_port $header

            set header [create_cluster_initial_header [expr {$max_packet_len + 1}] 4 $light]
            assert_cluster_bus_initial_header_rejected $cluster_port $header
            assert_equal PONG [R 0 ping]
        }
    }

    test "Packet with missing gossip messages don't cause invalid read" {
        set base_port [srv 0 port]
        set cluster_port [expr {$base_port + 10000}]
        set fake_node_id "abcdef1234567890abcdef1234567890abcdef12"

        # Get initial total messages received
        set info_before [R 0 cluster info]
        regexp {cluster_stats_messages_received:(\d+)} $info_before -> initial_received

        # Intentionally malformed packet: count=100 with no gossip data,
        # Create a packet with extensions=0 but CLUSTERMSG_FLAG0_EXT_DATA flag set
        # bogus extensions value, and EXT_DATA flag set.
        set packet [create_cluster_meet_packet $fake_node_id $base_port $cluster_port 100 2000000000 4]

        # Send the packet after configuring the socket to accept binary data
        set sock [socket 127.0.0.1 $cluster_port]
        fconfigure $sock -translation binary -buffering none -blocking 1
        puts -nonewline $sock $packet
        flush $sock
        close $sock

        wait_for_condition 1000 10 {
            [CI 0 cluster_stats_messages_received] == [expr {$initial_received + 1}]
        } else {
            fail "Packet was never received"
        }
    }
}

start_cluster 10 0 {tags {external:skip cluster tls:skip}} {
    test "Gossip count scales with higher percentage of `cluster-message-gossip-perc`" {
        R 0 config set cluster-message-gossip-perc 80

        set base_port [srv 0 port]
        set cluster_port [expr {$base_port + 10000}]
        set packet [create_cluster_meet_packet \
            "fakenode2234567890fakenode2234567890fake12" \
            $base_port $cluster_port 0 0 0]

        set sock [socket 127.0.0.1 $cluster_port]
        fconfigure $sock -translation binary -buffering full -blocking 1
        puts -nonewline $sock $packet
        flush $sock

        set hdr [read $sock 16]
        binary scan $hdr @4I totlen
        set rest [read $sock [expr {$totlen - 16}]]
        set reply "${hdr}${rest}"
        close $sock

        binary scan $reply @12Su type
        binary scan $reply @14Su count

        assert_equal 1 $type
        # The exact count depends on the membership-table state at the moment
        # the reply is crafted (the fake MEET node may or may not be counted
        # yet), so allow some slack. The property under test is scaling: at 80%
        # gossip-perc the count must be far above the default-perc baseline
        # of 1-2 entries.
        assert_morethan_equal $count 5
        assert_lessthan_equal $count 10
    }
}
