#!/bin/bash

# PARALLELISM=4
MAIN_CLASS="de.tudarmstadt.lt.flinkdt.CtGraphDT"
if [[ ! -z ${PARALLELISM} ]]; then
  PARALLELISM_PARM="-p ${PARALLELISM}"
fi

wordcount () {
  if [ ! -f hamlet.txt ]; then
    wget -O hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt
  fi
  rm -r wordcount-result.txt
  bin/flink run examples/WordCount.jar "file://$(pwd)/hamlet.txt" "file://$(pwd)/wordcount-result.txt"
}

if [[ ! -z $1 && $1 == "build" ]] || [[ ! -z $2 && $2 == "build" ]]; then
  mvn clean package -DskipTests -o -Pbuild-jar
fi

mkdir -p temp
cd temp
d=$(find . -maxdepth 1 -mindepth 1 -type d -name "flink-*" -print -quit | sed 's/.\///')

if [[ -z ${d} ]]; then
  wget 'http://ftp.halifax.rwth-aachen.de/apache/flink/flink-0.10.1/flink-0.10.1-bin-hadoop27-scala_2.11.tgz'
  tar -xzvf flink-*.tgz
  ##
  #
  # Note: consider to set some configuration parameter in conf/flink-conf.yaml
  # e.g.
  #   taskmanager.heap.mb: 20480
  #   taskmanager.numberOfTaskSlots: 20
  #   parallelism.default: 10
  #   taskmanager.network.numberOfBuffers: 4096
  #   taskmanager.tmp.dirs: ${HOME}/tmp
  #   akka.watch.heartbeat.pause: 600s
  #
  ##
  d=$(find . -maxdepth 1 -mindepth 1 -type d -name "flink-*" -print -quit | sed 's/.\///')
fi

echo "found $d"
cd $d
(exec "bin/start-local.sh")     # Start Flink in a new shell
echo "Check webgui on http://localhost:8081"
sleep 20

# test wordcount example or run app.conf
if [[ ! -z $1 && $1 == "test" ]] || [[ ! -z $2 && $2 == "test" ]]; then
  wordcount
else
  cd ../..
  temp/${d}/bin/flink run ${PARALLELISM_PARM} -c ${MAIN_CLASS} target/flinkdt-0.1.jar app.conf
  cd temp/$d
fi

(exec "bin/stop-local.sh")     # Stop flink, run command in a new shell
cd ../..
