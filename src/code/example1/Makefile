build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ZkConnector.java A.java B.java
runA:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. A localhost:2181
runB:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. B localhost:2181
clean:
	rm -f *.class
