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

import de.tudarmstadt.lt.flinkdt.Util
import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.types.{CT2def, CT2red}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object JoBimTextCPwExtractor extends App {

  def pipeline = {
    Checkpointed(
      Extractor(extractorfun, inputcolumn = DSTaskConfig.io_text_column) ~>
      N11Sum[CT2red[String, String], String, String](),
      out = DSTaskConfig.io_accumulated_AB,
      jobname = DSTaskConfig.jobname + "-1",
      reReadFromCheckpoint = true
    ) ~>
    Checkpointed(
      ComputeCT2[CT2red[String, String], CT2def[String, String], String, String](prune = true, sigfun = _.lmi, order = Order.DESCENDING),
      out = DSTaskConfig.io_accumulated_CT,
      jobname = DSTaskConfig.jobname + "-2",
      reReadFromCheckpoint = true
    ) ~>
    Checkpointed(
      ComputeDTSimplified.byJoin[CT2def[String, String], String, String](),
      out = DSTaskConfig.io_dt,
      jobname = DSTaskConfig.jobname + "-3",
      reReadFromCheckpoint = true
    ) ~>
    Checkpointed(
      FilterSortDT[CT2red[String, String], String, String](_.n11),
      out = DSTaskConfig.io_dt_sorted,
      jobname = DSTaskConfig.jobname + "-4",
      reReadFromCheckpoint = true
    )
  }

  var config = DSTaskConfig.resolveConfig(args)
  if(!config.hasPath("dt.jobname"))
    config = DSTaskConfig.resolveConfig(args ++ Array("-dt.jobname", getClass.getSimpleName.replaceAllLiterally("$","")))
  DSTaskConfig.load(config)

  def extractorfun:String => TraversableOnce[CT2red[String,String]] = Util.getExtractorfunFromJobname()

  // get input data
  val in = DSTaskConfig.io_text

  val ds = pipeline.process(input = DSTaskConfig.io_text)
  ds.first(10).print

}
