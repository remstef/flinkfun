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
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.fromElements(
//      """
//        To be, or not to be,--that is the question:--
//        Whether 'tis nobler in the mind to suffer
//        The slings and arrows of outrageous fortune
//        Or to take arms against a sea of troubles
      """
        The quick brown fox jumps over the lazy dog
        The quick brown cat jumps over the lazy dog
        The cat sleeps
      """)

    val jobims : DataSet[(String, String)] = text.filter(_ != null).filter(!_.trim().isEmpty()).flatMap(_.split("\\W+").sliding(3).map(x => (x(1), x(0) + " @ "  + x(2))))

    val cooc : DataSet[((String,String), Int)]= jobims.map((_, 1))
      .groupBy(0)
      .sum(1)

    val occ_jo = cooc.map( x => (x._1._1, x._2))
      .groupBy(0)
      .sum(1)

    val occ_bim = cooc.map( x => (x._1._2,  x._2))
      .groupBy(0)
      .sum(1)

    val join = cooc.map( x => (x._1._1, (x._1._2, x._2)))
      .join(occ_jo).where(0).equalTo(0){(c1, c2) => (c1._1, c1._2._1, c1._2._2, c2._2)}
      .map(x=>(x._2, (x._1, x._3, x._4)))
      .join(occ_bim).where(0).equalTo(0){(c1,c2) => (c1._2._1, c1._1, c1._2._2, c1._2._3, c2._2)}

    join print



  }
}
