#!/bin/bash

ZK_SCRIPT=~/myzk/bin/zkServer.sh
ZK_DATADIR=~/myzk/data-dir
ZK_HOST=localhost
ZK_PORT=8001

TIMEOUT=240

function start_zk() {
    echo "starting Zookeeper"
    ${ZK_SCRIPT} start
    sleep 3
}

function stop_zk() {
    ${ZK_SCRIPT} stop
    sleep 2
    /bin/rm -rf ${ZK_DATADIR}/*
    echo "stopped Zookeeper"
}

function start_cmd() {
    CMD="$1 &"
    FILEPREFIX=$2

    echo running ${CMD} ...

    eval ${CMD} > ${FILEPREFIX}.out 2> ${FILEPREFIX}.err
    echo $! > ${FILEPREFIX}.pid
    disown
}

function kill_proc() {
    NAME=$1
    kill `cat "${NAME}.pid"`
}

function start_fileserver() {
    NAME=$1
    start_cmd "./start_fileserver.sh ${ZK_HOST} ${ZK_PORT}" ${NAME}
    sleep 3
}

function start_jobtracker() {
    NAME=$1
    start_cmd "./start_jobtracker.sh ${ZK_HOST} ${ZK_PORT}" ${NAME}
    sleep 3
}

function start_worker() {
    NAME=$1
    start_cmd "./start_worker.sh ${ZK_HOST} ${ZK_PORT}" ${NAME}
}

function start_fileserver_injected() {
    NAME=$1
    start_cmd "injector/start_fileserver.sh ${ZK_HOST} ${ZK_PORT}" ${NAME}
    sleep 3
}

function start_jobtracker_injected() {
    NAME=$1
    start_cmd "injector/start_jobtracker.sh ${ZK_HOST} ${ZK_PORT}" ${NAME}
    sleep 3
}

function start_worker_injected() {
    NAME=$1
    start_cmd "injector/start_worker.sh ${ZK_HOST} ${ZK_PORT}" ${NAME}
}

function run() {
    sleep 5
    
    # submitting jobs
    for line in `cat jobs`; do
        ./submit_job.sh ${ZK_HOST} ${ZK_PORT} ${line}
    done

    sleep ${TIMEOUT}

    for line in `cat jobs`; do
        ./check_job_status.sh ${ZK_HOST} ${ZK_PORT} ${line} > ${line}.result
    done
}

