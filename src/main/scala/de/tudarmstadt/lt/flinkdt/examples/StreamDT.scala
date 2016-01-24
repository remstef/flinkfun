/*
 *  Copyright (c) 2016
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package de.tudarmstadt.lt.flinkdt.examples

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

import de.tudarmstadt.lt.flinkdt.textutils.TextToCT2
import de.tudarmstadt.lt.flinkdt.types.CT2red
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingTimeWindows, SlidingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

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
object StreamDT {

  def main(args: Array[String]) {

    val hostName = "localhost" //args(0)
    val port = 9999 //args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream(hostName, port)

    val ct2s = text.flatMap(TextToCT2.coocurrence(_))

    val occ = ct2s.keyBy(_.b)

    occ.map("jobim: " + _.toString()).print()

    val join = occ
      .join(occ).where(_.b).equalTo(_.b)
//      .window(SlidingTimeWindows.of(Time.of(10, TimeUnit.SECONDS), Time.of(10, TimeUnit.SECONDS)))
      .window(TumblingTimeWindows.of(Time.seconds(10)))
      .apply { (l, r) => CT2red(l.a, r.a) }

    val dt = join
      .keyBy("a","b")
      .sum("n11")

    dt//.filter(_.n11 > 1)
      .map("dtentry: " + _.toString())
      .print()

    println("---")

    env.execute("Scala SocketTextStreamWordCount Example")
  }

}
