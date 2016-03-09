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
import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2red}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object JoBimTextCP extends App {

  def pipeline = {
    Checkpointed(
      N11Sum[CT2red[String, String], String, String](),
      out = DSTaskConfig.out_accumulated_AB,
      jobname = DSTaskConfig.jobname + "-0",
      reReadFromCheckpoint = true
    ) ~>
    Checkpointed(
      ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String](prune = true, sigfun = _.lmi_n, order = Order.ASCENDING),
      out = DSTaskConfig.out_accumulated_CT,
      jobname = DSTaskConfig.jobname + "-1",
      reReadFromCheckpoint = true
    ) ~>
    Checkpointed(
      ComputeDTSimplified.byJoin[CT2ext[String, String], String, String](),
      out = DSTaskConfig.out_dt,
      jobname = DSTaskConfig.jobname + "-2",
      reReadFromCheckpoint = true
    ) ~>
    Checkpointed(
      FilterSortDT[CT2red[String, String], String, String](_.n11),
      out = DSTaskConfig.out_dt_sorted,
      jobname = DSTaskConfig.jobname + "-3",
      reReadFromCheckpoint = true
    )
  }

  def flipped_pipeline = {
    Checkpointed(
      // Begin: fliptask
      DSTask[CT2red[String, String], CT2red[String, String]](
        CtFromString[CT2red[String,String],String,String](_),
        ds => { ds.map(_.flipped().asInstanceOf[CT2red[String,String]]) },
        CtFromString[CT2red[String,String],String,String](_)
      ) ~>
      // End: fliptask
      ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String](prune = true, sigfun = _.lmi_n, order = Order.ASCENDING),
      out = s"${DSTaskConfig.out_accumulated_CT}-flipped",
      jobname = DSTaskConfig.jobname + "-1f",
      reReadFromCheckpoint = true
    ) ~>
      Checkpointed(
        ComputeDTSimplified.byJoin[CT2ext[String, String], String, String](),
        out = s"${DSTaskConfig.out_dt}-flipped",
        jobname = DSTaskConfig.jobname + "-2f",
        reReadFromCheckpoint = true
      ) ~>
      Checkpointed(
        FilterSortDT[CT2red[String, String], String, String](_.n11),
        out = s"${DSTaskConfig.out_dt_sorted}-flipped",
        jobname = DSTaskConfig.jobname + "-3f",
        reReadFromCheckpoint = true
      )
  }


  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val start = System.currentTimeMillis()
  var info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: -- \nduration: -- "
  DSTaskConfig.writeConfig(additional_comments = info)

  var ds = pipeline.process(input = DSTaskConfig.in_text)
  ds.first(10).print

  val dsf = flipped_pipeline.process(input = DSTaskConfig.out_accumulated_AB)
  dsf.first(10).print

  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info, overwrite = true)

}
