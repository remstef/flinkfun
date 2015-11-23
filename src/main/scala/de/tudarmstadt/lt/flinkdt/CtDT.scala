package de.tudarmstadt.lt.flinkdt

/**
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

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.math._

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object CtDT {
  def main(args: Array[String]) {

    var conf:Config = null
    if(args.length > 0)
      conf = ConfigFactory.parseFile(new File(args(0))).resolve() // load conf
    else
      conf = ConfigFactory.load() // load application.conf
    conf = conf.getConfig("DT")
    val outputconfig = conf.getConfig("output.ct")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    def writeIfExists[T <: Any](conf_path:String, ds:DataSet[CT2[T]]): Unit ={
      if(outputconfig.hasPath(conf_path)){
        val o = ds.map(_.toStringTuple())
        if(outputconfig.getString(conf_path) equals "stdout")
          o.print()
        else{
          o.writeAsCsv(outputconfig.getString(conf_path), "\n", "\t")
          if(!outputconfig.hasPath("dt") || conf_path == "dt") {
            env.execute("CtDT")
            return
          }
        }
      }
    }

    // get input data
    val in = conf.getString("input.text")

    var text:DataSet[String] = null
    if(new File(in).exists)
      text = env.readTextFile(in)
    else
      text = env.fromCollection(in.split('\n'))

    val ct_raw = text
      .filter(_ != null)
      .filter(!_.trim().isEmpty())
      .map(TextToCT2.ngram_patterns(_))
      .map(s => s.toIterable)
      .map(s => (s.size, Util.collapse(s).size))

    ct_raw.print()

    //writeIfExists("raw", ct_raw)

  }
}
