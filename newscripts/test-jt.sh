#!/bin/bash

source test-base.sh

if [ $# -ne 1 ]; then
    echo Please specify a profile under testcases.
    exit -1
fi

# testing setup
start_zk


ln -sf $1 inject-agent.properties

start_fileserver "fs"
start_jobtracker_injected "jt"
start_jobtracker "jt-backup"
for ((i=0;i<4;i+=1)); do
    start_worker "worker-$i"
done

run

kill_proc "fs"
kill_proc "jt"
kill_proc "jt-backup"
for ((i=0;i<4;i++)); do
    kill_proc "worker-$i"
done

stop_zk
killall java

grep -i found *.result
