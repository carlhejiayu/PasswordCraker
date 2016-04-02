#!/bin/bash
java -classpath ../src/lib/zookeeper-3.3.2.jar:../src/lib/log4j-1.2.15.jar:../src/.:injector/einject-0.0.1.jar:injector/asm-5.0.4.jar:.  -javaagent:injector/einject-0.0.1.jar Worker $1:$2
