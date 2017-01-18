## Some useful commands when working with flink

start a yarn session:

    bin/yarn-session.sh -qu longrunning -nm "Flink Session" -n 96 -s 1 -tm 2048 -jm 2048

start a scala shell on top of a yarn session

    # bin/start-scala-shell.sh -a <jar(s)> remote <jobmanager> <jobmanager-rpc-port>
    ${FLINK_HOME}/bin/start-scala-shell.sh -a target/lt.flinkdt-0.1.jar remote node-04d.ukp.informatik.tu-darmstadt.de 40220

load flinkdt classes into flink shell

    :load src/main/scripts/init.scala

start a flink yarn application 

    ${FLINK_HOME}/bin/flink run -m yarn-cluster -yn 192 -ys 1 -ytm 2048 -yqu shortrunning -c de.tudarmstadt.lt.flinkdt.pipes.SyntacticNgramExperimenter target/lt.flinkdt-0.1.jar googlesyntactics-app.conf fulldt

start a flink application on a running cluster (e.g. yarn session)

    ${FLINK_HOME}/bin/flink run -c de.tudarmstadt.lt.flinkdt.pipes.FullDT target/lt.flinkdt-0.1.jar hdfs-app.conf fulldt-hash-ngrams
    ${FLINK_HOME}/bin/flink run -p 100 -c de.tudarmstadt.lt.flinkdt.pipes.FullDT target/lt.flinkdt-0.1.jar hdfs-app.conf fulldt-hash-ngrams

build flink 

    # Switch Scala binary version to 2.11
    tools/change-scala-version.sh 2.11
    # Build with custom Scala version 2.11.4
    mvn clean install -DskipTests -Dscala.version=2.11.4
    
    mvn clean install -DskipTests -Pvendor-repos -Dhadoop.version=2.6.0-cdh5.4.9 -Dscala.version=2.11.7 -Drat.skip=true

