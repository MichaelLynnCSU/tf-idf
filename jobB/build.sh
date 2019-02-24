#!/bin/bash
export HADOOP_CLASSPATH=/usr/lib/jvm/java-openjdk/lib/tools.jar
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main unigram/*.java
jar cf unigram.jar unigram/*.class
rm -rf output/
