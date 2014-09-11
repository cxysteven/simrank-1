#!/bin/bash

##
export JAVA_HOME=/usr/java/default
ant

## prepare ws data and conf file
mkdir  ws
cd ws
normalize_jar="../lib/algo-normalize-2.0.19-1.jar"; #先要准备好归一化的jar包
#jar xvf $normalize_jar 
cd -

## run testcase
java -cp  ./lib/commons-logging-1.0.4.jar:./lib/junit-3.8.1.jar:./lib/jmock-2.6.0-RC1.jar:./lib/hamcrest-core-1.2RC2.jar:./lib/hadoop-0.19.0-core.jar:./build/classes/:dist/QueryPv.jar    test.com.taobao.research.jobs.querypv.TestAccumulate 

java -Djava.library.path=ws -cp  ./lib/commons-logging-1.0.4.jar:./lib/junit-3.8.1.jar:./lib/jmock-2.6.0-RC1.jar:./lib/hamcrest-core-1.2RC2.jar:./lib/hadoop-0.19.0-core.jar:./build/classes/:./lib/algo-normalize-jni.2.0.19-1.jar:lib/P4PPVLogParser.jar:dist/QueryPv.jar    test.com.taobao.research.jobs.querypv.TestQueryPv 

java -cp  ./lib/commons-logging-1.0.4.jar:./lib/junit-3.8.1.jar:./lib/jmock-2.6.0-RC1.jar:./lib/hamcrest-core-1.2RC2.jar:./lib/hadoop-0.19.0-core.jar:./build/classes/:dist/QueryPv.jar    test.com.taobao.research.jobs.querypv.TestQueryNorm 
