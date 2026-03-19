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
        # Only lz4 is valid
        r config set repl-compression-algo lz4
        set got [lindex [r config get repl-compression-algo] 1]
        assert_equal "lz4" $got
    }

    test {repl-compression-level round-trip consistency} {
        for {set i 0} {$i < 100} {incr i} {
            set val [expr {int(rand() * 1023) - 1000}]
            r config set repl-compression-level $val
            set got [lindex [r config get repl-compression-level] 1]
            assert_equal $val $got
        }
        # Restore default
        r config set repl-compression-level -5
    }

    test {Invalid enum values for repl-compression-algo are rejected} {
        set original [lindex [r config get repl-compression-algo] 1]
        foreach val {none snappy zstd gzip} {
            catch {r config set repl-compression-algo $val} err
            assert_match "*argument(s) must be one of the following*" $err
            set got [lindex [r config get repl-compression-algo] 1]
            assert_equal $original $got
        }
    }

    test {Compression level acceptance depends on range} {
        for {set i 0} {$i < 100} {incr i} {
            set val [expr {int(rand() * 2048) - 1024}]
            set in_range [expr {$val >= -1000 && $val <= 22}]

            set prev_level [lindex [r config get repl-compression-level] 1]
            set rc [catch {r config set repl-compression-level $val} err]

            if {$in_range} {
                assert_equal 0 $rc "Expected success for val=$val but got error: $err"
                set got [lindex [r config get repl-compression-level] 1]
                assert_equal $val $got
            } else {
                assert_equal 1 $rc "Expected failure for val=$val"
            }
        }
        # Restore default
        r config set repl-compression-level -5
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

            set info [$primary info replication]
            assert_match "*slave0:*" $info
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

            assert_equal {up} [s 0 master_link_status]

            $replica replicaof no one
        }
    }

    test {Backward compatibility - older replica without capa compression connects successfully} {
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
        assert_equal "lz4" [lindex [r config get repl-compression-algo] 1]
        assert_equal "-5" [lindex [r config get repl-compression-level] 1]
    }

    test {Invalid repl-compression-level range is rejected} {
        catch {r config set repl-compression-level -1001} err
        assert_match "*out of range*" $err
        catch {r config set repl-compression-level 23} err
        assert_match "*out of range*" $err
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
        r config set replcompression no
    }
}

}
