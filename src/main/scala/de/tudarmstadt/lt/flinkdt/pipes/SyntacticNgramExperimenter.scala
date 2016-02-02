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
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2def, CT2red}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

/**
  * Created by Steffen Remus
  *
  * flink run -m yarn-cluster -yn 192 -ys 1 -ytm 2048 -yqu shortrunning -c de.tudarmstadt.lt.flinkdt.pipes.SyntacticNgramExperimenter target/flinkdt-0.1.jar googlesyntactics-app.conf
  *
  */
object SyntacticNgramExperimenter extends App {

  val start = System.currentTimeMillis()

  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val setup_ct2ext = ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]()

  val default_jobimtext_pipeline = {
    Prune[String, String](sigfun = _.lmi_n, Order.ASCENDING) ~>
    /*  */
    ComputeDTSimplified.byJoin[CT2ext[String,String],String,String]() ~>
    DSWriter(DSTaskConfig.out_dt) ~>
    /*  */
    FilterSortDT[CT2red[String,String],String, String](_.n11)
    /*  */
  }

  val full_dt_pipeline = {
    /* minimal pruning */
    DSTask[CT2ext[String, String], CT2ext[String, String]](
      CtFromString[CT2ext[String,String],String,String](_),
      ds => { ds.filter(_.ndot1 > 1).filter(_.odot1 > 1) }
    ) ~>
    /* */
    ComputeDTSimplified.byJoin[CT2ext[String,String],String,String]() ~>
    DSWriter(DSTaskConfig.out_dt) ~>
    /* */
    FilterSortDT.apply[CT2red[String, String], String, String](_.n11)
  }

  val ct_location = in.stripSuffix("/") + ".ct2.acc.all"

  val ct_location_path:Path = new Path(ct_location)
  if(!ct_location_path.getFileSystem.exists(ct_location_path)) {
    setup_ct2ext.process(env, input = in, output = ct_location)
    env.execute(DSTaskConfig.jobname + "-prepare")
  }

  if(DSTaskConfig.jobname.contains("full"))
    full_dt_pipeline.process(env = env, input = ct_location, output = DSTaskConfig.out_dt_sorted)
  else
    default_jobimtext_pipeline.process(env = env, input = ct_location, output = DSTaskConfig.out_dt_sorted)

  env.execute(DSTaskConfig.jobname)

  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val info = s"start: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info)

}
