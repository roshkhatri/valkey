source tests/support/aofmanifest.tcl

tags {"rdb-compression external:skip needs:debug"} {

proc read_binary_file_prefix {path count} {
    set fd [open $path r]
    fconfigure $fd -translation binary
    set prefix [read $fd $count]
    close $fd
    return $prefix
}

proc read_binary_file {path} {
    set fd [open $path r]
    fconfigure $fd -translation binary
    set data [read $fd]
    close $fd
    return $data
}

proc write_binary_file {path data} {
    set fd [open $path w]
    fconfigure $fd -translation binary
    puts -nonewline $fd $data
    close $fd
}

proc dump_rdb_path {client} {
    return [file join [lindex [$client config get dir] 1] dump.rdb]
}

proc read_dump_rdb_header_bytes {client} {
    return [read_binary_file_prefix [dump_rdb_path $client] 8]
}

proc assert_lz4_rdb_envelope {client} {
    binary scan [read_binary_file_prefix [dump_rdb_path $client] 7] cu* bytes
    assert_equal [list 86 67 83 1 1 0 1] $bytes
}

proc write_rdb_test_dataset {client prefix} {
    $client flushall
    for {set i 0} {$i < 12} {incr i} {
        $client set "${prefix}:str:$i" [string repeat "${prefix}:value:$i " 16]
    }
    $client lpush "${prefix}:list" a b c d e
    $client sadd "${prefix}:set" alpha beta gamma
    $client zadd "${prefix}:zset" 1 one 2 two 3 three
    $client hset "${prefix}:hash" f1 v1 f2 [string repeat "${prefix}:hash " 8]
    $client xadd "${prefix}:stream" * f1 s1 f2 [string repeat "${prefix}:stream " 4]
    $client xadd "${prefix}:stream" * f1 s2 f2 tail
}

proc assert_rdb_test_dataset {client prefix} {
    assert_equal [string repeat "${prefix}:value:0 " 16] [$client get "${prefix}:str:0"]
    assert_equal 17 [$client dbsize]
    assert_equal 5 [$client llen "${prefix}:list"]
    assert_equal 3 [$client scard "${prefix}:set"]
    assert_equal 3 [$client zcard "${prefix}:zset"]
    assert_equal v1 [$client hget "${prefix}:hash" f1]
    assert_equal 2 [$client xlen "${prefix}:stream"]
}

start_server {overrides {save "" enable-debug-command local}} {
    test {RDB save and load round-trip with LZ4 compression} {
        set prefix "lz4-round-trip"
        r config set rdbcompression lz4
        write_rdb_test_dataset r $prefix
        assert_rdb_test_dataset r $prefix
        set digest [debug_digest]

        assert_equal "OK" [r save]
        assert_lz4_rdb_envelope r
        set loglines [count_log_lines 0]
        assert_equal "OK" [r debug reload nosave]
        verify_log_message 0 "*Logical RDB CRC64 skipped for streaming-compressed input*" $loglines
        r config rewrite
        restart_server 0 true false

        assert_equal "lz4" [lindex [r config get rdbcompression] 1]
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        assert_rdb_test_dataset r $prefix
    }

    test {Empty LZ4-compressed RDB saves and loads correctly} {
        r config set rdbcompression lz4
        r flushall

        assert_equal 0 [r dbsize]
        assert_equal "OK" [r save]
        r config rewrite
        assert_lz4_rdb_envelope r

        restart_server 0 true false

        assert_equal "lz4" [lindex [r config get rdbcompression] 1]
        assert_equal 0 [r dbsize]
    }

    test {RDB save with LZF (default) round-trips correctly} {
        set prefix "lzf-round-trip"
        r config set rdbcompression yes
        write_rdb_test_dataset r $prefix
        assert_rdb_test_dataset r $prefix

        set digest [debug_digest]
        assert_equal "OK" [r save]
        r config rewrite
        restart_server 0 true false

        assert_equal "yes" [lindex [r config get rdbcompression] 1]
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        assert_rdb_test_dataset r $prefix
    }

    test {Uncompressed RDB files load correctly (backward compat)} {
        set prefix "plain-rdb"
        r config set rdbcompression no
        write_rdb_test_dataset r $prefix
        assert_rdb_test_dataset r $prefix

        set digest [debug_digest]
        assert_equal "OK" [r save]
        r config rewrite
        restart_server 0 true false

        assert_equal "no" [lindex [r config get rdbcompression] 1]
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        assert_rdb_test_dataset r $prefix
    }

    test {Changing compression config during active BGSAVE does not affect the in-flight save} {
        r config set rdbcompression lz4
        r config set rdb-key-save-delay 10000
        r flushall
        for {set i 0} {$i < 128} {incr i} {
            r set "bgsave-race:$i" [string repeat "payload:$i " 128]
        }

        assert_match {*Background saving started*} [r bgsave]
        wait_for_condition 200 10 {
            [s rdb_bgsave_in_progress] eq 1
        } else {
            r config set rdb-key-save-delay 0
            fail "BGSAVE did not start in time"
        }

        # The child must keep the compression setting inherited at fork.
        r config set rdbcompression yes

        wait_for_condition 500 10 {
            [s rdb_bgsave_in_progress] eq 0
        } else {
            r config set rdb-key-save-delay 0
            fail "BGSAVE did not finish in time"
        }
        r config set rdb-key-save-delay 0

        assert_equal "yes" [lindex [r config get rdbcompression] 1]
        assert_lz4_rdb_envelope r

        assert_equal "OK" [r save]
        assert_equal "VALKEY" [string range [read_dump_rdb_header_bytes r] 0 5]

        r config set rdbcompression lz4
    }

    test {Switching from LZ4 to LZF preserves data} {
        set prefix "lz4-to-lzf"
        r config set rdbcompression lz4
        write_rdb_test_dataset r $prefix
        assert_rdb_test_dataset r $prefix
        set digest [debug_digest]

        # Save with LZ4, then restart with LZF and load the existing file.
        assert_equal "OK" [r save]
        r config set rdbcompression yes
        r config rewrite
        restart_server 0 true false

        assert_equal "yes" [lindex [r config get rdbcompression] 1]
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        assert_rdb_test_dataset r $prefix
    }

    test {Switching from LZF to LZ4 preserves data} {
        set prefix "lzf-to-lz4"
        r config set rdbcompression yes
        write_rdb_test_dataset r $prefix
        assert_rdb_test_dataset r $prefix
        set digest [debug_digest]

        # Save with LZF, then restart with LZ4 and load the existing file.
        assert_equal "OK" [r save]
        r config set rdbcompression lz4
        r config rewrite
        restart_server 0 true false

        assert_equal "lz4" [lindex [r config get rdbcompression] 1]
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        assert_rdb_test_dataset r $prefix
    }

    test {Invalid compression config is rejected} {
        set previous [lindex [r config get rdbcompression] 1]
        assert_error "*argument(s) must be one of the following: no, yes, lz4*" {
            r config set rdbcompression snappy
        }
        assert_equal $previous [lindex [r config get rdbcompression] 1]
    }

    test {Truncated LZ4 frame is rejected on load} {
        r config set rdbcompression lz4
        r flushall
        set noisy_payload ""
        for {set j 0} {$j < 32768} {incr j} {
            append noisy_payload [format %c [expr {(($j * 31) + 17) % 94 + 33}]]
        }
        for {set i 0} {$i < 128} {incr i} {
            r set "partial:$i" "${noisy_payload}:$i"
        }

        assert_equal "OK" [r save]
        set rdbfile [dump_rdb_path r]
        assert_lz4_rdb_envelope r

        set truncated [read_binary_file_prefix $rdbfile [expr {[file size $rdbfile] / 2}]]
        set fd [open $rdbfile w]
        fconfigure $fd -translation binary
        puts -nonewline $fd $truncated
        close $fd

        set failed [catch {r debug reload nosave} err]
        assert_equal 1 $failed
        assert_match "*Error trying to load the RDB*" $err

        r debug set-skip-checksum-validation 1
        set failed [catch {r debug reload nosave} err]
        r debug set-skip-checksum-validation 0
        assert_equal 1 $failed
        assert_match "*Error trying to load the RDB*" $err
    }

    test {LZ4 compressed RDB detects a content checksum mismatch} {
        r config set rdbcompression lz4
        assert_equal "yes" [lindex [r config get rdbchecksum] 1]
        r flushall
        for {set i 0} {$i < 100} {incr i} {
            r set "footer:$i" [string repeat "payload$i " 100]
        }

        r save
        set rdbfile [file join [lindex [r config get dir] 1] dump.rdb]
        set fd [open $rdbfile r+]
        fconfigure $fd -translation binary
        seek $fd -1 end
        binary scan [read $fd 1] cu checksum_byte
        seek $fd -1 end
        puts -nonewline $fd [binary format c [expr {$checksum_byte ^ 1}]]
        close $fd

        set failed [catch {r debug reload nosave} err]
        assert_equal 1 $failed
        assert_match "*Error trying to load the RDB*" $err

        set loglines [count_log_lines 0]
        r debug set-skip-checksum-validation 1
        assert_equal "OK" [r debug reload nosave]
        r debug set-skip-checksum-validation 0
        verify_log_message 0 "*Logical RDB CRC64 skipped for streaming-compressed input*" $loglines
    }

    test {RDB loader rejects incompatible VCS envelope fields without changing data} {
        r config set rdbcompression lz4
        r flushall
        r set incompatible-envelope:key [string repeat "payload " 100]

        assert_equal "OK" [r save]
        set digest [debug_digest]
        set rdbfile [dump_rdb_path r]
        set original [read_binary_file $rdbfile]

        foreach case {
            {version 3 2}
            {codec 4 127}
            {reserved-byte 5 1}
            {stream-kind 6 127}
        } {
            lassign $case field offset value
            set mutated [string replace $original $offset $offset [binary format c $value]]
            write_binary_file $rdbfile $mutated
            set loglines [count_log_lines 0]

            set failed [catch {r debug reload nosave} err]
            assert_equal 1 $failed "VCS $field should be rejected"
            assert_match "*Error trying to load the RDB*" $err
            verify_log_message 0 "*Invalid or unsupported RDB stream envelope*" $loglines
            assert_equal $digest [debug_digest]
        }

        write_binary_file $rdbfile $original
    }

    test {RDB loader rejects trailing data after an LZ4 frame} {
        r config set rdbcompression lz4
        r flushall
        r set trailing-data:key value
        assert_equal "OK" [r save]

        set rdbfile [dump_rdb_path r]
        set fd [open $rdbfile a]
        fconfigure $fd -translation binary
        puts -nonewline $fd "trailing-data"
        close $fd

        set loglines [count_log_lines 0]
        set failed [catch {r debug reload nosave} err]
        assert_equal 1 $failed
        assert_match "*Error trying to load the RDB*" $err
        verify_log_message 0 "*Compressed RDB stream*has trailing data*" $loglines
    }

    test {LZ4 compressed RDB detects corruption in compressed payload} {
        r config set rdbcompression lz4
        r flushall
        for {set i 0} {$i < 100} {incr i} {
            r set "corrupt:$i" [string repeat "testdata$i " 100]
        }

        assert_equal "OK" [r save]

        set rdbfile [file join [lindex [r config get dir] 1] dump.rdb]

        set fd [open $rdbfile r+]
        fconfigure $fd -translation binary
        set data [read $fd]
        set len [string length $data]
        set pos [expr {$len / 2}]
        set byte [string index $data $pos]
        binary scan $byte c val
        set newval [expr {($val + 1) & 0xFF}]
        set newbyte [binary format c $newval]
        set data [string replace $data $pos $pos $newbyte]
        seek $fd 0
        puts -nonewline $fd $data
        close $fd

        set failed [catch {r debug reload nosave} err]
        assert_equal 1 $failed
        assert_match "*Error trying to load the RDB*" $err
    }

}

start_server {config "minimal.conf" args {"--rdbcompression lz4"}} {
    test {Startup accepts valid LZ4 compression config} {
        assert_equal "lz4" [lindex [r config get rdbcompression] 1]
    }
}

start_server {overrides {save "" enable-debug-command local rdbchecksum no}} {
    test {LZ4 compressed RDB validates codec checksums when rdbchecksum is no} {
        r config set rdbcompression lz4
        r flushall
        for {set i 0} {$i < 50} {incr i} {
            r set "nocksum:$i" [string repeat "data$i " 100]
        }

        r save
        assert_lz4_rdb_envelope r
        set rdbfile [dump_rdb_path r]
        set original [read_binary_file $rdbfile]
        set digest [debug_digest]
        set loglines [count_log_lines 0]
        assert_equal "OK" [r debug reload nosave]
        verify_log_message 0 "*Logical RDB CRC64 skipped for streaming-compressed input*" $loglines

        set checksum_pos [expr {[string length $original] - 1}]
        binary scan [string index $original $checksum_pos] cu checksum_byte
        set corrupted [string replace $original $checksum_pos $checksum_pos \
                           [binary format c [expr {$checksum_byte ^ 1}]]]
        write_binary_file $rdbfile $corrupted

        set failed [catch {r debug reload nosave} err]
        assert_equal 1 $failed
        assert_match "*Error trying to load the RDB*" $err
        write_binary_file $rdbfile $original

        restart_server 0 true false
        set newdigest [debug_digest]
        assert {$digest eq $newdigest}
        assert_equal [string repeat "data10 " 100] [r get nocksum:10]
    }
}

start_server {overrides {save "" appendonly yes aof-use-rdb-preamble yes rdbcompression lz4}} {
    test {AOF rewrite RDB preamble remains plain with LZ4 stream snapshots} {
        r set aof-lz4:key [string repeat "aof-lz4-value " 100]
        set digest [debug_digest]

        r bgrewriteaof
        waitForBgrewriteaof r

        set base_aof [get_base_aof_path r]
        assert {[file exists $base_aof]}
        assert_equal "VALKEY" [string range [read_binary_file_prefix $base_aof 7] 0 5]

        restart_server 0 true false
        assert_equal $digest [debug_digest]
        assert_equal [string repeat "aof-lz4-value " 100] [r get aof-lz4:key]
    }
}

start_server {tags {"rdb-compression repl external:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]

    start_server {overrides {save "" enable-debug-command local}} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        test {Disk-based full sync writes plain RDB when rdbcompression is lz4} {
            $primary config set rdbcompression lz4
            $primary config set repl-diskless-sync no
            $primary config set rdb-del-sync-files no
            $primary flushall
            for {set i 0} {$i < 500} {incr i} {
                $primary set "repl:$i" [string repeat "payload$i " 40]
            }

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            # wait_for_sync only checks master_link_status; the replica may
            # still be loading the RDB. Wait until loading completes before
            # comparing digests.
            wait_done_loading $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after LZ4 RDB full sync"
            }

            assert_equal [string repeat "payload42 " 40] [$replica get repl:42]
            assert {[file exists [dump_rdb_path $primary]]}
            assert_equal "VALKEY" [string range [read_dump_rdb_header_bytes $primary] 0 5]

            $primary set repl:post-sync "after-sync"
            wait_for_condition 50 100 {
                [$replica get repl:post-sync] eq "after-sync"
            } else {
                fail "Replica did not receive post-sync write"
            }
        }

        test {Diskless full sync remains compatible when rdbcompression is lz4} {
            $replica replicaof no one
            $primary config set repl-diskless-sync yes
            $primary config set repl-diskless-sync-delay 0
            $replica config set repl-diskless-load swapdb
            $primary flushall
            for {set i 0} {$i < 300} {incr i} {
                $primary set "diskless:$i" [string repeat "payload$i " 60]
            }

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            # wait_for_sync only checks master_link_status; the replica may
            # still be loading the RDB. Wait until loading completes before
            # comparing digests.
            wait_done_loading $replica

            wait_for_condition 50 100 {
                [status $replica master_link_status] eq "up" &&
                [$primary debug digest] eq [$replica debug digest]
            } else {
                fail "Replica digest mismatch after diskless LZ4 full sync"
            }

            assert_equal [string repeat "payload42 " 60] [$replica get diskless:42]
            $primary config set repl-diskless-sync no
        }

        $replica replicaof no one
    }
}

}
