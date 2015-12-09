#!/bin/bash

MAIN_CLASS="de.tudarmstadt.lt.flinkdt.task.Executor"

##
#
# make sure 'ssh localhost' is working
#
# ( System Preferences -> Sharing -> Remote Login )
#
##

start-dfs.sh
##
# hdfs namenode
# hdfs datanode
##

##
# optional
##
hdfs namenode -format

start-yarn.sh
## 
# yarn resourcemanager
# yarn nodemanager
##

##
# start flink yarn session
##
temp/temp/flink-0.10.1/bin/yarn-session.sh -n 1

##
# run jobs from either file:// or hdfs:// 
##
temp/temp/flink-0.10.1/bin/flink run -c ${MAIN_CLASS} target/flinkdt-0.1.jar app.conf

