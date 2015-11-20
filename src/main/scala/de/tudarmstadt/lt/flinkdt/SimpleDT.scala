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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

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
object SimpleDT {
  def main(args: Array[String]) {

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

    val jobims_raw = text
      .filter(_ != null)
      .filter(!_.trim().isEmpty())
      .flatMap(Text2JoBim.ngram_patterns(_))

    val jobims_accumulated = jobims_raw
      .groupBy("jo","bim")
      .sum("freq_cooc")
      .filter(_.freq_cooc > 1)

    //jobims_accumulated.map(jb => (jb.jo, jb.bim, jb.freq_cooc)).writeAsCsv("simplewiki.jobims.tsv", "\n", "\t")

    val joined = jobims_accumulated
      .joinWithHuge(jobims_accumulated)
      .where("bim")
      .equalTo("bim")

     //joined.map(j => (j._1.jo, j._2.jo, j._1.bim, j._1.freq_cooc, j._2.freq_cooc)).writeAsCsv("s1_simplewiki.joined_jobims.tsv", "\n", "\t")

    case class DTEntry(jo1 : String, jo2 : String, freq : Int)
    val dt = joined.map(x=>((x._1.jo, x._2.jo), 1))
      .groupBy(0)
      .sum(1)
      .map(x => DTEntry(x._1._1, x._1._2, x._2))
      .filter(_.freq > 1)

    val dtsort = dt
      .groupBy("jo1")
      .sortGroup("freq", Order.DESCENDING)
      .first(100)

    dtsort.map(dt => (dt.jo1, dt.jo2, dt.freq)).writeAsCsv("data/s1patterns.dt.tsv", "\n", "\t")

    dtsort.print()

    env.execute("DT")




  }
}
