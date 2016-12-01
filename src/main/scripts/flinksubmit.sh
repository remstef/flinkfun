#!/bin/bash

###
#
##

export FLINK_HOME=$HOME/local/flink-1.1.3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export HADOOP_HOME=/opt/cloudera/parcels/CDH-5.4.11-1.cdh5.4.11.p0.5 

jar=$HOME/lt.flinkdt_1.1.3_2.11-0.4.jar 
class=de.uhh.lt.flink.JoinJBT
appargs="-in hdfs:///user/remus/wiki.en/enwiki-20151201-oie-jb-count-min2 -out hdfs:///user/remus/wiki.en/enwiki-20151201-oie-jb-joinedFT-min2"
memjobmanager=1024
memtaskmanager=2048
numtaskmanager=300
queue=shortrunning

# run in new session
$FLINK_HOME/bin/flink run -m yarn-cluster -yjm $memjobmanager -yn $numtaskmanager -ys 1 -ytm $memtaskmanager -yqu $queue -c $class $jar $appargs

# run local
jar=$HOME/git/flinkfun/target
appargs="-in file:///home/rem/data/wiki.en/enwiki-20151201-oie-jb-count-min2 -out hdfs:///home/rem/data/wiki.en/enwiki-20151201-oie-jb-joinedF-min2"

##
# # flink session
# $FLINK_HOME/bin/flink-session -n 10  -qu $queue -tm 2048
#
# # flink-shell
# flink-shell -a $HOME/git/flinkfun/target/lt.flinkdt_1.0.0_2.11-0.4.jar remote 10.70.21.252 56786
# 
# # run in session
# flink run -m 10.70.21.21:38226 -c de.tudarmstadt.lt.seg.app.FlinkParse $(pwd)/git/lt.kd/lt.n2n-v2-preprocess/target/lt.n2n-preprocess-0.0.1-SNAPSHOT.jar -f "hdfs:///user/remus/wiki.en/enwiki-20151201-pages-articles" -o "hdfs:///user/remus/wiki.en/enwiki-20151201-pages-articles-syntactic2" -l
##
