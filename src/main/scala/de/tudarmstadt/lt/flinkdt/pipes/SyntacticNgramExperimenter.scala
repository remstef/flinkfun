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
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2def, CT2red}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

/**
  * Created by Steffen Remus
  *
  * flink run -m yarn-cluster -yn 192 -ys 1 -ytm 2048 -yqu shortrunning -c de.tudarmstadt.lt.flinkdt.pipes.SyntacticNgramExperimenter target/flinkdt-0.1.jar googlesyntactics-app.conf
  *
  */
object SyntacticNgramExperimenter extends App {

  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val start = System.currentTimeMillis()

  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  var info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: -- \nduration: -- "
  DSTaskConfig.writeConfig(additional_comments = info)

  // get input data
  val in = DSTaskConfig.io_text

  val setup_ct2ext = ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]()

  def fliptask = DSTask[CT2ext[String, String], CT2ext[String, String]](
    ds => { ds.map(_.flipped().asInstanceOf[CT2ext[String,String]]) }
  )

  val default_jobimtext_pipeline = {
    Checkpointed(
      Checkpointed(
        Prune[String, String](sigfun = _.lmi_n, Order.DESCENDING) ~>
          /*  */
          ComputeDTSimplified.byJoin[CT2ext[String,String],String,String](),
        DSTaskConfig.io_dt
      ) ~>
        /*  */
        FilterSortDT[CT2red[String,String],String, String](_.n11),
      DSTaskConfig.io_dt_sorted
    )
  }

  val default_jobimtext_pipeline_flipped = {
    Checkpointed(
      Checkpointed(
        fliptask ~>
        Prune[String, String](sigfun = _.lmi_n, Order.DESCENDING) ~>
          /*  */
          ComputeDTSimplified.byJoin[CT2ext[String,String],String,String](),
        s"${DSTaskConfig.io_dt}-flipped"
      ) ~>
        /*  */
        FilterSortDT[CT2red[String,String],String, String](_.n11),
      s"${DSTaskConfig.io_dt_sorted}-flipped"
    )
  }

  val full_dt_pipeline = {
    Checkpointed(
      Checkpointed(
        /* minimal pruning */
        DSTask[CT2ext[String, String], CT2ext[String, String]](
          ds => { ds.filter(_.ndot1 > 1).filter(_.odot1 > 1) }
        ) ~>
          /* */
          ComputeDTSimplified.byJoin[CT2ext[String,String],String,String](),
        DSTaskConfig.io_dt
      ) ~>
        /* */
        FilterSortDT.apply[CT2red[String, String], String, String](_.n11),
      DSTaskConfig.io_dt_sorted
    )
  }

  val full_dt_pipeline_flipped = {
    Checkpointed(
      Checkpointed(
        fliptask ~>
        /* minimal pruning */
        DSTask[CT2ext[String, String], CT2ext[String, String]](
          ds => { ds.filter(_.ndot1 > 1).filter(_.odot1 > 1) }
        ) ~>
          /* */
          ComputeDTSimplified.byJoin[CT2ext[String,String],String,String](),
        s"${DSTaskConfig.io_dt}-flipped"
      ) ~>
        /* */
        FilterSortDT.apply[CT2red[String, String], String, String](_.n11),
      s"${DSTaskConfig.io_dt_sorted}-flipped"
    )
  }

  val ct_location = in.stripSuffix("/") + ".ct2.acc.all"

  val ct_location_path:Path = new Path(ct_location)
  if(!ct_location_path.getFileSystem.exists(ct_location_path)) {
    setup_ct2ext.process(input = in, output = ct_location, jobname = DSTaskConfig.jobname + "-prepare")
  }

  if(DSTaskConfig.jobname.contains("full")) {
    full_dt_pipeline.process(input = ct_location)
    full_dt_pipeline_flipped.process(input = ct_location)
  }
  else {
    default_jobimtext_pipeline.process(input = ct_location)
    default_jobimtext_pipeline_flipped.process(input = ct_location)
  }

  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info)

}
