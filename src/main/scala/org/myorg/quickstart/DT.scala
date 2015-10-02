package org.myorg.quickstart

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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.math._
import scala.reflect.io.File

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
object DT {
  def main(args: Array[String]) {

    var conf:Config = null
    if(args.length > 0)
      conf = ConfigFactory.load(args(0)) // load conf
    else
      conf = ConfigFactory.load() // load application.conf
    conf = conf.getConfig("DT")
    val outputconfig = conf.getConfig("outfile")
    if(!(outputconfig.hasPath("jobim") && outputconfig.hasPath("dt")))
      return

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val in = conf.getString("input")

    var text:DataSet[String] = null
    if(File(in).exists)
      text = env.readTextFile(in)
    else
      text = env.fromCollection(in.split('\n'))

    case class JoBim (jo: String, bim: String, freq_cooc: Int = 0, freq_jo: Int = 0, freq_bim: Int = 0, sig: Double = 0d)
    val jobims_raw = text
      .filter(_ != null)
      .filter(!_.trim().isEmpty())
      .filter(_.split("\\W+").length >= 3)
      .flatMap(_.split("\\W+")
        .sliding(3)
        .map{ x => JoBim(x(1), x(0) + " @ "  + x(2), 1, 1, 1)})

    val jobims_accumulated = jobims_raw.groupBy("jo","bim")
      .sum("freq_cooc")
      .filter(_.freq_cooc > 1)

    val jos_accumulated = jobims_raw.groupBy("jo")
      .sum("freq_jo")
      .map(_.copy(bim="@"))
      .filter(_.freq_jo > 1)

    val bims_accumulated = jobims_raw.groupBy("bim")
      .sum("freq_bim")
      .map(_.copy(jo="@"))
      .filter(jb => jb.freq_bim > 1 && jb.freq_bim <= 1000)

    def lmi(jb: JoBim, n:Long):JoBim = {
      val pmi = (log(jb.freq_cooc) + log(n)) - (log(jb.freq_jo) + log(jb.freq_bim))
      val lmi = jb.freq_cooc * pmi
      jb.copy(sig = pmi)
    }

    val n = jobims_accumulated.map(_.freq_cooc).reduce(_+_).collect()(0);
    println(n)

    val jobimsall = jobims_accumulated
      .joinWithHuge(jos_accumulated)
      .where("jo")
      .equalTo("jo")((jb1, jb2) => jb1.copy(freq_jo=jb2.freq_jo))
      .joinWithHuge(bims_accumulated)
      .where("bim")
      .equalTo("bim")((jb1, jb2) => jb1.copy(freq_bim = jb2.freq_bim))
      .map(lmi(_, n))
      .groupBy("jo")
      .sortGroup("sig", Order.DESCENDING)
      .first(1000)

    if(outputconfig.hasPath("jobim")){
      val o = jobimsall.map(jb => (jb.jo, jb.bim, jb.freq_cooc, jb.freq_jo, jb.freq_bim, f"${jb.sig}%.4f"))
      if(outputconfig.getString("jobim") equals "stdout")
        o.print()
      else{
        o.writeAsCsv(outputconfig.getString("jobim"), "\n", "\t")
        if(!outputconfig.hasPath("dt")) {
          env.execute("JOBIMS")
          return
        }
      }
    }

    val joined = jobimsall
      .joinWithHuge(jobimsall)
      .where("bim")
      .equalTo("bim")

    case class DTEntry(jo1 : String, jo2 : String, freq : Int)
    val dt = joined.map(x=>DTEntry(x._1.jo, x._2.jo, 1 ))
      .groupBy("jo1", "jo2")
      .sum("freq")
      .filter(_.freq > 1)

    val dtsort = dt
      .groupBy("jo1")
      .sortGroup("freq", Order.DESCENDING)
      .first(100)

    if(outputconfig.hasPath("dt")){
      val o = dtsort.map(dt => (dt.jo1, dt.jo2, dt.freq));
      if(outputconfig.getString("dt") equals "stdout")
        o.print()
      else {
        o.writeAsCsv(outputconfig.getString("dt"), "\n", "\t")
        env.execute("DT")
      }
    }

  }
}
