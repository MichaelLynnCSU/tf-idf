#!/bin/bash
export HADOOP_CLASSPATH=/usr/lib/jvm/java-openjdk/lib/tools.jar
$HADOOP_HOME/bin/hdfs dfs -rm -r /testLoc/out*
$HADOOP_HOME/bin/hadoop jar unigram.jar unigram.UnigramDriver /testLoc/cs435data /testLoc/output
