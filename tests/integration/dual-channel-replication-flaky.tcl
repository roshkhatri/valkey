proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

proc wait_process_paused idx {
    set pid [srv $idx pid]
    wait_for_condition 50 1000 {
        [string match "T*" [exec ps -o state= -p $pid]]
    } else {
        fail "Process $pid didn't stop, current state is [exec ps -o state= -p $pid]"
    }
}

proc wait_and_resume_process idx {
    set pid [srv $idx pid]
    wait_process_paused $idx
    resume_process $pid
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica1 [srv 0 client]
    set replica1_host [srv 0 host]
    set replica1_port [srv 0 port]
    set replica1_log [srv 0 stdout]
    start_server {} {
        set replica2 [srv 0 client]
        set replica2_host [srv 0 host]
        set replica2_port [srv 0 port]
        set replica2_log [srv 0 stdout]
        start_server {} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]
            set backlog_size [expr {10 ** 6}]
            set loglines [count_log_lines -1]

            $primary config set repl-diskless-sync yes
            $primary config set dual-channel-replication-enabled yes
            $primary config set repl-backlog-size $backlog_size
            $primary config set loglevel debug
            $primary config set repl-timeout 10
            $primary config set rdb-key-save-delay 10
            populate 1024 primary 16
            
            set load_handle0 [start_write_load $primary_host $primary_port 20]

            $replica1 config set dual-channel-replication-enabled yes
            $replica2 config set dual-channel-replication-enabled yes
            $replica1 config set loglevel debug
            $replica2 config set loglevel debug
            $replica1 config set repl-timeout 10
            $replica2 config set repl-timeout 10

            # Pause replicas after primary forks for
            $replica1 debug pause-after-fork 1
            $replica2 debug pause-after-fork 1
            test "2000 1 - Test dual-channel: primary tracking replica backlog refcount - start with empty backlog" {
                $replica1 replicaof $primary_host $primary_port
                set start_time [clock milliseconds]
                set res [wait_for_log_messages 0 {"*Add rdb replica * no repl-backlog to track*"} $loglines 2000 1]
                set end_time [clock milliseconds]
                set elapsed [expr {$end_time - $start_time}]
                puts "Execution time: $elapsed ms"
                set res [wait_for_log_messages 0 {"*Attach replica rdb client*"} $loglines 20 100]
                set loglines [lindex $res 1]
                incr $loglines
                wait_and_resume_process -2
                verify_replica_online $primary 0 700
                wait_for_condition 50 1000 {
                    [status $replica1 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
                $replica1 replicaof no one
                assert [string match *replicas_waiting_psync:0* [$primary info replication]]
            }

            test "2000 1 - Test dual-channel: primary tracking replica backlog refcount - start with backlog" {
                $replica2 replicaof $primary_host $primary_port
                set start_time [clock milliseconds]
                set res [wait_for_log_messages 0 {"*Add rdb replica * tracking repl-backlog tail*"} $loglines 2000 1]
                set end_time [clock milliseconds]
                set elapsed [expr {$end_time - $start_time}]
                puts "Execution time: $elapsed ms"
                set loglines [lindex $res 1]
                incr $loglines
                wait_and_resume_process -1
                verify_replica_online $primary 0 700
                wait_for_condition 50 1000 {
                    [status $replica2 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
                assert [string match *replicas_waiting_psync:0* [$primary info replication]]
            }

            stop_write_load $load_handle0
        }
    }
}

start_server {tags {"dual-channel-replication external:skip"}} {
    set replica1 [srv 0 client]
    set replica1_host [srv 0 host]
    set replica1_port [srv 0 port]
    set replica1_log [srv 0 stdout]
    start_server {} {
        set replica2 [srv 0 client]
        set replica2_host [srv 0 host]
        set replica2_port [srv 0 port]
        set replica2_log [srv 0 stdout]
        start_server {} {
            set primary [srv 0 client]
            set primary_host [srv 0 host]
            set primary_port [srv 0 port]
            set backlog_size [expr {10 ** 6}]
            set loglines [count_log_lines -1]

            $primary config set repl-diskless-sync yes
            $primary config set dual-channel-replication-enabled yes
            $primary config set repl-backlog-size $backlog_size
            $primary config set loglevel debug
            $primary config set repl-timeout 10
            $primary config set rdb-key-save-delay 10
            populate 1024 primary 16
            
            set load_handle0 [start_write_load $primary_host $primary_port 20]

            $replica1 config set dual-channel-replication-enabled yes
            $replica2 config set dual-channel-replication-enabled yes
            $replica1 config set loglevel debug
            $replica2 config set loglevel debug
            $replica1 config set repl-timeout 10
            $replica2 config set repl-timeout 10

            # Pause replicas after primary forks for
            $replica1 debug pause-after-fork 1
            $replica2 debug pause-after-fork 1
            test "20 100 - Test dual-channel: primary tracking replica backlog refcount - start with empty backlog" {
                $replica1 replicaof $primary_host $primary_port
                set start_time [clock milliseconds]
                if {[catch {
                    set res [wait_for_log_messages 0 {"*Add rdb replica * no repl-backlog to track*"} $loglines 20 100]
                } err]} {
                    set elapsed [expr {[clock milliseconds] - $start_time}]
                    puts "Execution time: $elapsed ms (failed)"
                    error $err
                }
                set elapsed [expr {[clock milliseconds] - $start_time}]
                puts "Execution time: $elapsed ms"
                set res [wait_for_log_messages 0 {"*Attach replica rdb client*"} $loglines 20 100]
                set loglines [lindex $res 1]
                incr $loglines
                wait_and_resume_process -2
                verify_replica_online $primary 0 700
                wait_for_condition 50 1000 {
                    [status $replica1 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
                $replica1 replicaof no one
                assert [string match *replicas_waiting_psync:0* [$primary info replication]]
            }

            test "20 100 - Test dual-channel: primary tracking replica backlog refcount - start with backlog" {
                $replica2 replicaof $primary_host $primary_port
                set start_time [clock milliseconds]
                if {[catch {
                    set res [wait_for_log_messages 0 {"*Add rdb replica * tracking repl-backlog tail*"} $loglines 20 100]
                } err]} {
                    set elapsed [expr {[clock milliseconds] - $start_time}]
                    puts "Execution time: $elapsed ms (failed)"
                    error $err
                }
                set elapsed [expr {[clock milliseconds] - $start_time}]
                puts "Execution time: $elapsed ms" 
                set loglines [lindex $res 1]
                incr $loglines
                wait_and_resume_process -1
                verify_replica_online $primary 0 700
                wait_for_condition 50 1000 {
                    [status $replica2 master_link_status] == "up"
                } else {
                    fail "Replica is not synced"
                }
                assert [string match *replicas_waiting_psync:0* [$primary info replication]]
            }

            stop_write_load $load_handle0
        }
    }
}