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
import org.apache.flink.core.fs.Path

/**
  * Created by Steffen Remus
  *
  * flink run -m yarn-cluster -yn 192 -ys 1 -ytm 2048 -yqu shortrunning -c de.tudarmstadt.lt.flinkdt.pipes.SyntacticNgramExperimenter target/flinkdt-0.1.jar googlesyntactics-app.conf
  *
  */
object SyntacticNgramExperimenterPruned extends App {

  val start = System.currentTimeMillis()

  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  // get input data
  val in = DSTaskConfig.in_text

  val ct_location = in.stripSuffix("/") + ".ct2.acc.all.pruned"

  val setup_ct2ext = {
    ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String](prune = true, sigfun = _.lmi_n, Order.ASCENDING) ~>
    GraphWriter[CT2ext[String,String], String, String](s"${ct_location}-graph")
  }

  val ct_location_path:Path = new Path(ct_location)
  if(!ct_location_path.getFileSystem.exists(ct_location_path)) {
    setup_ct2ext.process(input = in, output = ct_location, jobname = DSTaskConfig.jobname + "-prepare")
  }

  val jobimtext_pipeline = {
    /*  */
    ComputeDTSimplified.byJoin[CT2ext[String,String],String,String]() ~>
    GraphWriter[CT2red[String,String],String, String](s"${DSTaskConfig.out_dt}-graph") ~>
    /*  */
    FilterSortDT[CT2red[String,String],String, String](_.n11) ~>
    /*  */
    GraphWriter[CT2red[String,String],String, String](s"${DSTaskConfig.out_dt_sorted}-graph")
  }.process(input = ct_location)


  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val info = s"start: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info)

}