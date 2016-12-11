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

package de.tudarmstadt.lt.flinkdt.pipes

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date
import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.Implicits._
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2red}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.reflect.ClassTag
import de.tudarmstadt.lt.flinkdt.types.CT2
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/**
  * Created by Steffen Remus.
  */
object RCountCT {
  
  

  def exec_pipeline(env:ExecutionEnvironment): Unit = {
    type T = String
    case class CT3 (a:T, b:T, c:T)
    case class CT3r(a:T, b:T, c:T, nabc:Double)
    
    //    val raw = env.readCsvFile[CT3](DSTaskConfig.io_ctraw, "\t", includedFields = Array(1,2,3)) // first field is src

    val raw = env.readTextFile(DSTaskConfig.io_ctraw).map { _.split("\t") }.filter { _.length == 4 }.map { _ match { case Array(src,a,b,c) => CT3r(a,b,c,1d) } }
    
    val nabc = raw
      .groupBy("a","b","c")
      .sum("nabc")
      .checkpointed(DSTaskConfig.io_basedir("nabc"), DSTaskConfig.jobname("(1/8) [nabc]"), true, env)

    nabc
      .groupBy("a","b")
      .sum("nabc")
      .map { c => c.copy(c="*") }
      .save(DSTaskConfig.io_basedir("nab"), DSTaskConfig.jobname("(2/8) [nab]"))
      
    nabc
      .groupBy("b","c")
      .sum("nabc")
      .map { c => c.copy(a="*") }
      .save(DSTaskConfig.io_basedir("nbc"), DSTaskConfig.jobname("(3/8) [nbc]"))
      
    nabc
      .groupBy("a","c")
      .sum("nabc")
      .map { c => c.copy(b="*") }
      .save(DSTaskConfig.io_basedir("nac"), DSTaskConfig.jobname("(4/8) [nac]"))
      
    nabc
      .groupBy("a")
      .sum("nabc")
      .map { c => c.copy(b="*", c="*") }
      .save(DSTaskConfig.io_basedir("na"), DSTaskConfig.jobname("(5/8) [na]"))
      
    nabc
      .groupBy("b")
      .sum("nabc")
      .map { c => c.copy(a="*", c="*") }
      .save(DSTaskConfig.io_basedir("nb"), DSTaskConfig.jobname("(6/8) [nb]"))
      
    nabc
      .groupBy("c")
      .sum("nabc")
      .map { c => c.copy(a="*", b="*") }
      .save(DSTaskConfig.io_basedir("nc"), DSTaskConfig.jobname("(7/8) [nc]"))
      
    nabc
      .sum("nabc")
      .map { c => c.copy(a="*", b="*", c="*") }
      .save(DSTaskConfig.io_basedir("n"), DSTaskConfig.jobname("(8/8) [n]"))
      
  }
  
  def main(args: Array[String]): Unit = {
  
    DSTaskConfig.load(DSTaskConfig.resolveConfig(args))
  
    val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
    val start = System.currentTimeMillis()
    var info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: -- \nduration: -- "
    val config_path = DSTaskConfig.writeConfig(additional_comments = info)
    println(DSTaskConfig.toString())
    println(config_path)
  
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    if(DSTaskConfig.config.hasPath("parallelism")){
      val cores = DSTaskConfig.config.getInt("parallelism")
      println(s"Setting parallelism to $cores.")
      env.setParallelism(cores)
    }
    
    exec_pipeline(env = env)//.first(3).print
  
    val end = System.currentTimeMillis()
    val dur = Duration.ofMillis(end-start)
    info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
    DSTaskConfig.writeConfig(dest = config_path, additional_comments = info, overwrite = true)
  
  }

}
