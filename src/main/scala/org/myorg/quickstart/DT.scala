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

import com.typesafe.config.ConfigFactory
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

    val conf = ConfigFactory.load() // load application.conf

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

//    // get input data
//    val text = env.fromElements(
////      """
////        To be, or not to be,--that is the question:--
////        Whether 'tis nobler in the mind to suffer
////        The slings and arrows of outrageous fortune
////        Or to take arms against a sea of troubles
//      """
//        the quick brown fox jumps over the lazy dog
//        the quick brown cat jumps over the lazy dog
//        the cat sleeps again
//        the dog sleeps too
//        but the cat sleeps more
//        and the dog sleeps deeper
//        and the quick brown fox really jumps over the ultra lazy dog
//        HEY WHATS UP??
//      """)

    val text = env.readTextFile("/Volumes/ExtendedHD/Users/stevo/Documents/corpora/simplewiki/simplewikipedia_sent_tok.txt")
//    val text = env.readTextFile("/Volumes/ExtendedHD/Users/stevo/Documents/corpora/simplewiki/simplewikipedia_sent_tok_fruits.txt")
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
      .filter(_.freq_cooc >= 2)

    val jos_accumulated = jobims_raw.groupBy("jo")
      .sum("freq_jo")
      .map(_.copy(bim="@"))
      .filter(_.freq_jo >= 2)

    val bims_accumulated = jobims_raw.groupBy("bim")
      .sum("freq_bim")
      .map(_.copy(jo="@"))
      .filter(_.freq_bim >= 2)

//    jobims_accumulated.map(jb => (jb.jo, jb.bim, jb.freq_cooc)).writeAsCsv("simplewiki.jobim.tsv", "\n", "\t")
//    jos_accumulated.map(jb => (jb.jo, jb.freq_jo)).writeAsCsv("simplewiki.jo.tsv", "\n", "\t")
//    bims_accumulated.map(jb => (jb.bim, jb.freq_bim)).writeAsCsv("simplewiki.bim.tsv", "\n", "\t")

    def lmi(jb: JoBim, n:Long):JoBim = {
      val pmi = (log(jb.freq_cooc) + log(n)) - (log(jb.freq_jo) + log(jb.freq_bim))
      val lmi = jb.freq_cooc * pmi
      //val lmi = jb.freq_cooc.toDouble * Math.log(jb.freq_cooc / (jb.freq_jo.toDouble * jb.freq_bim.toDouble))
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

    jobimsall.map(jb => (jb.jo, jb.bim, jb.freq_cooc, jb.freq_jo, jb.freq_bim, f"${jb.sig}%.4f")).writeAsCsv("simplewiki.jobimall.tsv", "\n", "\t")

    val joined = jobimsall
      .joinWithHuge(jobimsall)
      .where("bim")
      .equalTo("bim")

//    joined.map(jb => (jb.jo, jb.jbim, j._1.bim, j._1.freq, j._2.freq)).writeAsCsv("simplewiki.joined_jobims.tsv", "\n", "\t")

    case class DTEntry(jo1 : String, jo2 : String, freq : Int)
    val dt = joined.map(x=>DTEntry(x._1.jo, x._2.jo, 1 ))
      .groupBy("jo1", "jo2")
      .sum("freq")
      .filter(_.freq > 1)

    val dtsort = dt
      .groupBy("jo1")
      .sortGroup("freq", Order.DESCENDING)
      .first(100)

    dtsort.map(dt => (dt.jo1, dt.jo2, dt.freq)).writeAsCsv("simplewiki.dt.tsv", "\n", "\t")
//
//    dtsort.print()
    env.execute("DT")




  }
}
