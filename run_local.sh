#!/bin/bash

wordcount () {
  if [ ! -f temp/${d}/hamlet.txt ]; then
    wget -O temp/${d}/hamlet.txt http://www.gutenberg.org/cache/epub/1787/pg1787.txt
  fi
  rm -r temp/${d}/wordcount-result.txt
  temp/${d}/bin/flink run temp/${d}/examples/WordCount.jar "file://$(pwd)/temp/${d}/hamlet.txt" "file://$(pwd)/temp/${d}/wordcount-result.txt"
}

if [[ ! -z $1 && $1 == "build" ]] || [[ ! -z $2 && $2 == "build" ]]; then
  mvn clean package -DskipTests -o -Pbuild-jar
fi

mkdir -p temp
cd temp
d=$(find . -type d -name "flink-*" -maxdepth 1 -mindepth 1 -print -quit)

if [[ -z ${d} ]]; then
  wget 'http://mirror.23media.de/apache/flink/flink-0.10.0/flink-0.10.0-bin-hadoop2-scala_2.10.tgz'
  tar -xzvf flink-*.tgz
  d=$(find . -type d -name "flink-*" -maxdepth 1 -mindepth 1 -print -quit)
fi

echo "found $d"
cd ..
sh temp/${d}/bin/start-local.sh    # Start Flink

# test wordcount example or run app.conf
if [[ ! -z $1 && $1 == "test" ]] || [[ ! -z $2 && $2 == "test" ]]; then
  wordcount
else
  temp/${d}/bin/flink run -c de.tudarmstadt.lt.flinkdt.CtDT target/flinkdt-0.1.jar app.conf
fi

sh temp/${d}/bin/stop-local.sh
