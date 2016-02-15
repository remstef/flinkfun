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

package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import de.tudarmstadt.lt.flinkdt.textutils.TextToCT2
import de.tudarmstadt.lt.flinkdt.types._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object CtDT extends App {

  val config:Config =
    if(args.length > 0)
      ConfigFactory.parseFile(new File(args(0))).withFallback(ConfigFactory.load()).resolve() // load conf with fallback to default application.conf
    else
      ConfigFactory.load() // load default application.conf

  val jobname = getClass.getSimpleName.replaceAllLiterally("$","")
  val config_dt = config.getConfig("dt")
  val outputconfig = config_dt.getConfig("output.ct")
  val outputbasedir = new File(if(config_dt.hasPath("output.basedir")) config_dt.getString("output.basedir") else "./", s"out-${jobname}")
  if(!outputbasedir.exists())
    outputbasedir.mkdirs()
  val pipe = outputconfig.getStringList("pipeline").toArray

  def writeIfExists[T1 <: Any, T2 <: Any](conf_path:String, ds:DataSet[CT2def[T1,T2]], stringfun:((CT2def[T1,T2]) => String) = ((ct2:CT2def[T1,T2]) => ct2.toString)): Unit = {
    if(outputconfig.hasPath(conf_path)){
      val o = ds.map(stringfun).map(Tuple1(_))
      if(outputconfig.getString(conf_path) equals "stdout") {
        o.print()
      }
      else{
        o.writeAsCsv(new File(outputbasedir, outputconfig.getString(conf_path)).getAbsolutePath, "\n", "\t")
        if(pipe(pipe.size-1) == conf_path) {
          env.execute(jobname)
          return
        }
      }
    }
  }

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = config_dt.getString("input.text")

  val text:DataSet[String] = if(new File(in).exists) env.readTextFile(in) else env.fromCollection(in.split('\n'))

  val ct_accumulated:DataSet[CT2def[String, String]] = text
    .filter(_ != null)
    .filter(!_.trim().isEmpty())
    .flatMap(s => TextToCT2.ngram_patterns(s,5,3))
    .groupBy("a","b")
    .sum("n11")
    .filter(_.n11 > 1)
    .map(_.asCT2def())

  writeIfExists("accAB", ct_accumulated)

  val ct_accumulated_white = if(config_dt.hasPath("input.whitelist") && new File(config_dt.getString("input.whitelist")).exists) {
    val whitelist = env.readTextFile(config_dt.getString("input.whitelist")).map(Tuple1(_)).distinct(0)
    val white_cts_A = ct_accumulated // get all contexts of whitelist terms
      .joinWithTiny(whitelist)
      .where("a").equalTo(0)((x, y) =>  x )
      .distinct(0)
    val white_cts_B_from_white_cts_A = ct_accumulated
      .joinWithTiny(white_cts_A)
      .where("b").equalTo("b")((x,y) => x) // get all terms of contexts of whitelist terms
    writeIfExists("accABwhite", white_cts_B_from_white_cts_A)
    white_cts_B_from_white_cts_A
  }else{
    ct_accumulated
  }

  val ct_accumulated_A = ct_accumulated_white.map(ct => {ct.n1dot=ct.n11; ct})
    .groupBy("a")
    .reduce((l,r) => {l.n1dot = l.n1dot+r.n1dot; l})
    .filter(_.n1dot > 1)

  writeIfExists("accA", ct_accumulated_A)

  val ct_accumulated_B = ct_accumulated_white
    .map(ct => {ct.ndot1 = ct.n11; ct.n = 1f; ct}) // misuse n as odot1 i.e. the number of distinct occurrences of feature B (parameter wc=wordcount or wpfmax=wordsperfeature in traditional jobimtext)
    .groupBy("b")
    .reduce((l,r) => {l.ndot1 = l.ndot1 + r.ndot1; l.n = l.n + r.n; l})
    .filter(ct => ct.n <= 1000 && ct.n > 1)

  writeIfExists("accB", ct_accumulated_B)

  val n = ct_accumulated_white.map(ct => ct.n11).reduce(_+_)
  val ct_accumulated_n = ct_accumulated_white.crossWithTiny(n)((ct,n) => {ct.n = n; ct})

  val ct_all = ct_accumulated_n
    .join(ct_accumulated_A)
    .where("a")
    .equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
    .join(ct_accumulated_B)
    .where("b")
    .equalTo("b")((x, y) => { x.ndot1 = y.ndot1; x })
    .map(ct => {ct.n11 = ct.lmi(); ct}) // misuse n11 as lmi score

  writeIfExists("accall", ct_all)

  val ct_all_filtered = ct_all
    .groupBy("a")
    .sortGroup("n11", Order.DESCENDING)
    .first(1000)

  val joined:DataSet[(CT2def[String,String], CT2def[String,String])] = ct_all_filtered
    .join(ct_all_filtered)
    .where("b")
    .equalTo("b")

  val dt = joined.map(cts => CT2def[String,String](cts._1.a, cts._2.a, n11=1f))
    .groupBy("a", "b")
    .sum("n11")
    .filter(_.n11 > 1)

  val dtf = dt
    .groupBy("a")
    .sum("n1dot")
    .filter(_.n1dot > 2)

  val dtsort = dt
    .join(dtf)
    .where("a").equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
    .groupBy("a")
    .sortGroup("n11", Order.DESCENDING)
    .first(200)

  writeIfExists("dt", dtsort)


}
