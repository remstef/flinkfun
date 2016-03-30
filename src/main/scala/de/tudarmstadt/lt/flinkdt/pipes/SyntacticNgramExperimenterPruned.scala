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
  *
  * flink run -m yarn-cluster -yn 192 -ys 1 -ytm 2048 -yqu shortrunning -c de.tudarmstadt.lt.flinkdt.pipes.SyntacticNgramExperimenter target/flinkdt-0.1.jar googlesyntactics-app.conf
  *
  */
object SyntacticNgramExperimenterPruned extends App {

  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val start = System.currentTimeMillis()

  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  var info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: -- \nduration: -- "
  DSTaskConfig.writeConfig(additional_comments = info)

  // get input data
  val in = DSTaskConfig.in_text

  def fliptask = DSTask[CT2red[String, String], CT2red[String, String]](
    CtFromString[CT2red[String,String],String,String](_),
    ds => { ds.map(_.flipped().asInstanceOf[CT2red[String,String]]) },
    CtFromString[CT2red[String,String],String,String](_)
  )

  val jobimtext_pipeline = {
    Checkpointed(
      Checkpointed(
        Checkpointed(
          ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String](prune = true, _.lmi_n, Order.DESCENDING),
          DSTaskConfig.out_accumulated_CT
        ) ~>
          /* */
          ComputeDTSimplified.byJoin[CT2ext[String,String],String,String](),
        DSTaskConfig.out_dt
      ) ~>
        /*  */
        FilterSortDT[CT2red[String,String],String, String](_.n11),
      DSTaskConfig.out_dt_sorted
    )
  }

  val jobimtext_pipeline_flipped = {
    Checkpointed(
      Checkpointed(
        fliptask ~>
          Checkpointed(
            ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String](prune = true, _.lmi_n, Order.DESCENDING),
            s"${DSTaskConfig.out_accumulated_CT}-flipped"
          ) ~>
          /*  */
          ComputeDTSimplified.byJoin[CT2ext[String,String],String,String](),
        s"${DSTaskConfig.out_dt}-flipped"
      ) ~>
        /*  */
        FilterSortDT[CT2red[String,String],String, String](_.n11),
      s"${DSTaskConfig.out_dt_sorted}-flipped"
    )
  }

  jobimtext_pipeline.process(input = in)
  jobimtext_pipeline_flipped.process(input = in)

  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info, overwrite = true)

}
