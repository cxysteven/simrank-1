#!/bin/bash
##for check
export temppath=$1
cd $temppath/src
#export JAVA_HOME=/home/a/project/java
export JAVA_HOME=/usr/java/jdk1.6.0_13
/home/ads/tools/antx/dist/antx/bin/ant  QueryPv
mv dist/QueryPv.jar ../jar/QueryPv.jar
cd ../rpm
/usr/local/bin/rpm_create -p /home/a -v $3 -r $4 $2.spec