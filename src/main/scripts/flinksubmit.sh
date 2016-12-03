#!/bin/bash

###
#
##

export FLINK_HOME=$HOME/local/flink-1.1.3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=$HADOOP_CONF_DIR
export HADOOP_HOME=/opt/cloudera/parcels/CDH-5.4.11-1.cdh5.4.11.p0.5 

jar=$HOME/lt.flinkdt_*-jar-with-cluster-dependencies.jar
class=de.uhh.lt.flink.JoinJBX
appargs="-in hdfs:///user/remus/wiki.en/enwiki-20151201-oie-jb-count-min2 -out hdfs:///user/remus/wiki.en/enwiki-20151201-oie-jb-joinedFX-min2 -tmpdir hdfs:///user/remus/wiki.en/enwiki-20151201-oie-jb-min2-joinXtmp"
memjobmanager=1024
memtaskmanager=4096
numtaskmanager=100
queue=shortrunning

# run in new session
$FLINK_HOME/bin/flink run -m yarn-cluster -yjm $memjobmanager -yn $numtaskmanager -ys 1 -ytm $memtaskmanager -yqu $queue -c $class $jar $appargs

# run local
jar=$HOME/git/flinkfun/target/lt.flinkdt_*-jar-with-local-dependencies.jar
export JAVA_OPTS="-Xmx4g"
class=de.uhh.lt.flink.JoinJBD
appargs="-parallelism 8 -in file:///home/rem/data/wiki.en/enwiki-20151201-oie-jb-count-min2 -out file:///home/rem/data/wiki.en/enwiki-20151201-oie-jb-joinedF-min2"
class=de.tudarmstadt.lt.flinkdt.pipes.ImpliCtJBT
appargs="-parallelism 8 -dt.io.ct.raw file:///home/rem/data/wiki.en/enwiki-20151201-oie-jb-count-min2 -dt.io.dir file:///home/rem/data/wiki.en/enwiki-20151201-oie-jb-count-min2-DT"

$JAVA_HOME/bin/java -cp $jar $JAVA_OPTS $class $appargs


##
# # flink session

$FLINK_HOME/bin/yarn-session.sh -n $numtaskmanager  -qu $queue -tm $memtaskmanager

#
# # flink-shell
$FLINK_HOME/bin/start-scala-shell.sh -a $jar remote 10.70.21.23 38079

# 
# # run in session
# flink run -m 10.70.21.21:38226 -c de.tudarmstadt.lt.seg.app.FlinkParse $(pwd)/git/lt.kd/lt.n2n-v2-preprocess/target/lt.n2n-preprocess-0.0.1-SNAPSHOT.jar -f "hdfs:///user/remus/wiki.en/enwiki-20151201-pages-articles" -o "hdfs:///user/remus/wiki.en/enwiki-20151201-pages-articles-syntactic2" -l
##
