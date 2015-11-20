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
object DT {
  def main(args: Array[String]) {

    var conf:Config = null
    if(args.length > 0)
      conf = ConfigFactory.parseFile(new File(args(0))).resolve() // load conf
    else
      conf = ConfigFactory.load() // load application.conf
    conf = conf.getConfig("DT")
    val outputconfig = conf.getConfig("output")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    def writeIfExists(conf_path:String, ds:DataSet[JoBim]): Unit ={
      if(outputconfig.hasPath(conf_path)){
        val o = ds.map(jb => (jb.jo, jb.bim, jb.freq_cooc, jb.freq_jo, jb.freq_bim, f"${jb.freq_sig}%.4f", jb.freq_distinct_jo, jb.freq_distinct_bim, f"${jb.freq_distinct_sig}%.4f"))
        if(outputconfig.getString(conf_path) equals "stdout")
          o.print()
        else{
          o.writeAsCsv(outputconfig.getString(conf_path), "\n", "\t")
          if(!outputconfig.hasPath("dt")) {
            env.execute("JOBIMS")
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

    val jobims_raw = text
      .filter(_ != null)
      .filter(!_.trim().isEmpty())
      .flatMap(Text2JoBim.ngram_patterns(_))

    writeIfExists("jbraw", jobims_raw)

    val jobims_accumulated = jobims_raw.groupBy("jo","bim")
      .sum("freq_cooc")
      .filter(_.freq_cooc > 1)

    writeIfExists("jbacc", jobims_accumulated)

    val jos_accumulated = jobims_raw.map(jb => {jb.freq_jo=jb.freq_cooc; jb})
      .groupBy("jo")
      .reduce((j1,j2)=> j1.copy(freq_jo=j1.freq_jo+j2.freq_jo, freq_distinct_jo=j1.freq_distinct_jo+j2.freq_distinct_jo))
      .map(jb => {jb.bim="@"; jb})
      .filter(_.freq_jo > 1)

    writeIfExists("joacc", jos_accumulated)

    val bims_accumulated = jobims_raw.map(jb => {jb.freq_bim=jb.freq_cooc; jb})
      .groupBy("bim")
      .reduce((j1,j2)=>j1.copy(freq_bim=j1.freq_bim+j2.freq_bim, freq_distinct_bim=j1.freq_distinct_bim+j2.freq_distinct_bim))
      .map(jb => {jb.jo="@"; jb})
      .filter(jb => jb.freq_bim > 1)

    writeIfExists("bimacc", bims_accumulated)

    def lmi(jb: JoBim, n:Long, n_distinct:Long):JoBim = {
      val pmi = (log(jb.freq_cooc) + log(n)) - (log(jb.freq_jo) + log(jb.freq_bim))
      val lmi = jb.freq_cooc * pmi

      val distinct_pmi = (log(jb.freq_cooc) + log(n_distinct)) - (log(jb.freq_distinct_jo) + log(jb.freq_distinct_bim))
      val distinct_lmi = jb.freq_cooc * distinct_pmi

      jb.freq_sig = pmi
      jb.freq_distinct_sig = distinct_pmi

      jb
    }

    val (n,n_distinct) = jobims_accumulated.map(jb => (jb.freq_cooc,1)).reduce((f1,f2) => (f1._1+f2._1, f1._2+f2._2)).collect()(0);
    println(n)
    println(n_distinct)

    val jobimsall = jobims_accumulated
      .joinWithHuge(jos_accumulated)
      .where("jo")
      .equalTo("jo")((jb1, jb2) => {jb1.freq_jo=jb2.freq_jo; jb1.freq_distinct_jo=jb2.freq_distinct_jo; jb1})
      .joinWithHuge(bims_accumulated)
      .where("bim")
      .equalTo("bim")((jb1, jb2) => {jb1.freq_bim=jb2.freq_bim; jb1.freq_distinct_bim=jb2.freq_distinct_bim; jb1})
      .map(lmi(_, n, n_distinct))


    writeIfExists("jbaccall", jobimsall)

    env.execute("DT")
    return

    val jobimsall_filtered = jobimsall.filter(jb => jb.freq_bim > 1 && jb.freq_bim <= 1000)
      .groupBy("jo")
      .sortGroup("freq_sig", Order.DESCENDING)
      .first(1000)

    val joined = jobimsall_filtered
      .joinWithHuge(jobimsall_filtered)
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
