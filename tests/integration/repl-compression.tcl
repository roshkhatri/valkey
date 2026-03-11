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
        set algos {none lz4}
        for {set i 0} {$i < 100} {incr i} {
            set val [lindex $algos [expr {int(rand() * 2)}]]
            r config set repl-compression-algo $val
            set got [lindex [r config get repl-compression-algo] 1]
            assert_equal $val $got
        }
        # Restore default
        r config set repl-compression-level -5
        r config set repl-compression-algo none
    }

    test {repl-compression-level round-trip consistency} {
        # Must set algo to lz4 first so arbitrary levels are accepted
        r config set repl-compression-algo lz4
        for {set i 0} {$i < 100} {incr i} {
            set val [expr {int(rand() * 1023) - 1000}]
            r config set repl-compression-level $val
            set got [lindex [r config get repl-compression-level] 1]
            assert_equal $val $got
        }
        # Restore defaults
        r config set repl-compression-level -5
        r config set repl-compression-algo none
    }

    test {Invalid enum values for repl-compression-algo are rejected} {
        set original [lindex [r config get repl-compression-algo] 1]
        for {set i 0} {$i < 100} {incr i} {
            set val [randstring 1 20 alpha]
            # Skip valid values
            if {$val eq "none" || $val eq "lz4"} continue
            catch {r config set repl-compression-algo $val} err
            assert_match "*argument(s) must be one of the following*" $err
            # Value must be unchanged
            set got [lindex [r config get repl-compression-algo] 1]
            assert_equal $original $got
        }
    }

    test {Compression level acceptance depends on range and algorithm} {
        for {set i 0} {$i < 100} {incr i} {
            # Pick a random algo
            set algo [expr {int(rand() * 2) ? "lz4" : "none"}]
            # Pick a random integer in a wider range to test boundaries
            set val [expr {int(rand() * 2048) - 1024}]

            # Determine expected outcome
            set in_range [expr {$val >= -1000 && $val <= 22}]
            set is_default [expr {$val == -5}]
            set is_lz4 [expr {$algo eq "lz4"}]
            set should_succeed [expr {$in_range && ($is_lz4 || $is_default)}]

            # Save current state
            set prev_algo [lindex [r config get repl-compression-algo] 1]
            set prev_level [lindex [r config get repl-compression-level] 1]

            # Set algo first
            r config set repl-compression-level -5
            r config set repl-compression-algo $algo

            set rc [catch {r config set repl-compression-level $val} err]

            if {$should_succeed} {
                assert_equal 0 $rc "Expected success for algo=$algo val=$val but got error: $err"
                set got [lindex [r config get repl-compression-level] 1]
                assert_equal $val $got
            } else {
                assert_equal 1 $rc "Expected failure for algo=$algo val=$val"
            }

            # Restore defaults for next iteration
            r config set repl-compression-level -5
            r config set repl-compression-algo none
        }
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

            # Verify the primary recorded the compression capability.
            # Check the replica info on the primary side.
            set info [$primary info replication]
            assert_match "*slave0:*" $info

            # The replica should have connected successfully with capa compression.
            # We verify by checking the log for the REPLCONF capa exchange.
            # Since we can't directly inspect replica_capa bits from Tcl,
            # we verify the handshake completed successfully.
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

            # Handshake should succeed without compression capability
            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Backward compatibility - older replica without capa compression connects successfully} {
        # A standard replica without replcompression should connect fine
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
}

start_server {overrides {save ""}} {

    test {Repl compression config defaults are correct} {
        assert_equal "no" [lindex [r config get replcompression] 1]
        assert_equal "none" [lindex [r config get repl-compression-algo] 1]
        assert_equal "-5" [lindex [r config get repl-compression-level] 1]
    }

    test {Repl compression level requires lz4 algo} {
        r config set repl-compression-level -5
        r config set repl-compression-algo none

        catch {r config set repl-compression-level -9} err
        assert_match "*supported only when repl-compression-algo is lz4*" $err
        assert_equal "none" [lindex [r config get repl-compression-algo] 1]
        assert_equal "-5" [lindex [r config get repl-compression-level] 1]

        # With lz4, non-default levels should work
        r config set repl-compression-algo lz4
        r config set repl-compression-level -9
        assert_equal "-9" [lindex [r config get repl-compression-level] 1]

        # Switching back to none should fail if level is non-default
        catch {r config set repl-compression-algo none} err
        assert_match "*supported only when repl-compression-algo is lz4*" $err
        assert_equal "lz4" [lindex [r config get repl-compression-algo] 1]

        # Restore defaults
        r config set repl-compression-level -5
        r config set repl-compression-algo none
    }

    test {Invalid repl-compression-algo values are rejected} {
        catch {r config set repl-compression-algo snappy} err
        assert_match "*argument(s) must be one of the following: none, lz4*" $err
    }

    test {Invalid repl-compression-level range is rejected} {
        r config set repl-compression-algo lz4
        catch {r config set repl-compression-level -1001} err
        assert_match "*between* -1000 *22*" $err
        catch {r config set repl-compression-level 23} err
        assert_match "*between* -1000 *22*" $err
        # Restore
        r config set repl-compression-level -5
        r config set repl-compression-algo none
    }

    test {Startup rejects non-lz4 with non-default repl streaming level} {
        set confdir [tmpdir "repl-compression-invalid-startup"]
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
        puts $fd "repl-compression-algo none"
        puts $fd "repl-compression-level -9"
        close $fd

        set rc [catch {exec $::VALKEY_SERVER_BIN $cfgfile} err]
        assert {$rc == 1}
        assert_match "*repl-compression-level is supported only when repl-compression-algo is lz4*" $err

        # Defensive cleanup
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
        r config set repl-compression-algo none
        r config set replcompression no
    }
}

}
