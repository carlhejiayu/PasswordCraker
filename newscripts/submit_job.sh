#!/bin/bash
java -classpath ../src/lib/zookeeper-3.3.2.jar:../src/lib/log4j-1.2.15.jar:../src/. -Dlog4j.configuration=file://$HOME/myzk/conf/log4j.properties ClientDriver $1:$2 job $3
