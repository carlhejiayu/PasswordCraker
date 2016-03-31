build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar *.java
run $1:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. $1 localhost:2181
clean:
	rm -f *.class
