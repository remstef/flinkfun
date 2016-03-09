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

import de.tudarmstadt.lt.flinkdt.tasks.{N11Sum, Extractor, ComputeCT2, DSTaskConfig}
import de.tudarmstadt.lt.flinkdt.Implicits._
import de.tudarmstadt.lt.flinkdt.textutils.{TextToCT2, CtFromString}
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2def, CT2red, CT2}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus.
  */
object ImplictJBT extends App {

  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val start = System.currentTimeMillis()
  var info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: -- \nduration: -- "
  DSTaskConfig.writeConfig(additional_comments = info)

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  val ds:DataSet[String] = env.readTextFile(DSTaskConfig.in_text)
  val dsct:DataSet[CT2red[String,String]] = ds
    .flatMap(s => TextToCT2.ngrams(s, 3))
    .checkpointed(DSTaskConfig.out_raw, CtFromString[CT2red[String,String], String,String], DSTaskConfig.jobname("1"), true)


  val dsctext:DataSet[CT2ext[String,String]] = dsct
    .applyTask(ComputeCT2[CT2red[String,String], CT2ext[String,String], String,String]())
    .checkpointed(DSTaskConfig.out_accumulated_CT, CtFromString[CT2ext[String,String], String,String], DSTaskConfig.jobname("2"), true)

  dsct.first(3).prettyprint

  dsctext.first(3).prettyprint


  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info, overwrite = true)

}
