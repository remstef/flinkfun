package de.tudarmstadt.lt.flinkdt

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.helper.Time
import org.apache.flink.util.Collector
import org.objenesis.instantiator.gcj.GCJInstantiatorBase.DummyStream

/**
 * This example shows an implementation of WordCount with data from a text socket. 
 * To run the example make sure that the service providing the text data is already up and running.
 *
 * To start an example socket text stream on your local machine run netcat from a command line, 
 * where the parameter specifies the port number:
 *
 * {{{
 *   nc -lk 9999
 * }}}
 *
 * Usage:
 * {{{
 *   SocketTextStreamWordCount <hostname> <port> <output path>
 * }}}
 *
 * This example shows how to:
 *
 *   - use StreamExecutionEnvironment.socketTextStream
 *   - write a simple Flink Streaming program in scala.
 *   - write and use user-defined functions.
 */
object SocketTextStreamWordCount {

  def main(args: Array[String]) {
//    if (args.length != 2) {
//      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
//      return
//    }
    
    val hostName = "localhost" //args(0)
    val port = 9999 //args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream(hostName, port)
    val jobims = text.flatMap(_.split("\\W+").sliding(3).map(x => (x(1), x(0) + " @ "  + x(2))))

    val cooc = jobims.map((_, 1))
      .groupBy(0)
      .sum(1)

    val occ_jo = jobims.map(x=>(x._1, 1))
      .groupBy(0)
      .sum(1)

    val occ_bim = jobims.map( x => (x._2, 1))
      .groupBy(0)
      .sum(1)

    val join = cooc
      .map( x => (x._1._1, (x._1._2, x._2)))
      .join(occ_jo).onWindow(30, TimeUnit.SECONDS).every(5, TimeUnit.SECONDS).where(0).equalTo(0){(c1, c2) => (c1._1, c1._2._1, c1._2._2, c2._2)}
      .map(x=>(x._2, (x._1, x._3, x._4)))
      .join(occ_bim).onWindow(2, TimeUnit.SECONDS).every(5, TimeUnit.SECONDS).where(0).equalTo(0){(c1,c2) => (c1._2._1, c1._1, c1._2._2, c1._2._3, c2._2)}


      join.print()


    env.execute("Scala SocketTextStreamWordCount Example")
  }

}
