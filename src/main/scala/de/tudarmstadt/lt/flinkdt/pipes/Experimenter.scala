/*
 *  Copyright (c) 2015
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

import de.tudarmstadt.lt.flinkdt.types.CT2red
import de.tudarmstadt.lt.flinkdt.Util
import de.tudarmstadt.lt.flinkdt.tasks._
import org.apache.flink.api.scala._


/**
  * Created by Steffen Remus
  */
object Experimenter extends App {

  def process() = {

    { /* */
      //WhiteListFilter.CT2Min[String, String](DSTaskConfig.in_whitelist, env) ~|~>
      /* */
      //      ComputeDTSimplified.CT2MinJoin[String,String]() ~> DSWriter(DSTaskConfig.out_dt)
      ComputeDTSimplified.byGraph[CT2red[String,String],String,String]() ~> DSWriter(DSTaskConfig.io_dt, s"${DSTaskConfig.jobname}-process")
      /* */
    }.process(input = s"${DSTaskConfig.io_accumulated_AB}")


  }

  def preprocess() = {

    { /* */
      Extractor(extractorfun, inputcolumn = DSTaskConfig.io_text_column) ~|~>
      /*  */
      N11Sum[CT2red[String,String], String, String]
      /*  */
    }.process(input = in, output = s"${DSTaskConfig.io_accumulated_AB}", jobname = s"${DSTaskConfig.jobname}-preprocess")

  }

  def postprocess() = {

    { /* */
      FilterSortDT[CT2red[String,String], String, String](_.n11)
      /* */
    }.process(input = DSTaskConfig.io_dt, output = DSTaskConfig.io_dt_sorted, jobname = s"${DSTaskConfig.jobname}-postprocess")

  }

  var config = DSTaskConfig.resolveConfig(args)
  if(!config.hasPath("dt.jobname"))
    config = DSTaskConfig.resolveConfig(args ++ Array("-dt.jobname", getClass.getSimpleName.replaceAllLiterally("$","")))
  DSTaskConfig.load(config)

  val start = System.currentTimeMillis()
  def extractorfun:String => TraversableOnce[CT2red[String,String]] = Util.getExtractorfunFromJobname()

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.io_text

//  val preprocess_output_path:Path = new Path(DSTaskConfig.out_accumulated_AB)
//  if(!preprocess_output_path.getFileSystem.exists(preprocess_output_path))
  preprocess()

  process()

  postprocess()

  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val info = s"start: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info)

}
