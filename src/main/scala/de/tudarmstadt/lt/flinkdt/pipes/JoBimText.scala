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

import de.tudarmstadt.lt.flinkdt.types.{CT2def, CT2red}
import de.tudarmstadt.lt.flinkdt.Util
import de.tudarmstadt.lt.flinkdt.tasks._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object JoBimText extends App {

  def process() = {

    { /* */
      //WhiteListFilter.CT2[String, String](DSTaskConfig.in_whitelist, env) ~|~>
      /* */
      ComputeCT2[CT2red[String,String], CT2def[String,String], String, String](prune = true, sigfun = _.lmi, order = Order.DESCENDING) ~>
      DSWriter[CT2def[String,String]](DSTaskConfig.io_accumulated_CT, s"${DSTaskConfig.jobname}-process") ~>
      /*  */
      ComputeDTSimplified.byJoin[CT2def[String,String],String,String]()
      /* */
    }.process(input = DSTaskConfig.io_accumulated_AB, output =  DSTaskConfig.io_dt, jobname = s"${DSTaskConfig.jobname}-process")

  }

  def preprocess() = {

    { /* */
      Extractor(extractorfun, inputcolumn = DSTaskConfig.io_text_column) ~>
      /*  */
      N11Sum[CT2red[String,String], String, String]()
      /*  */
    }.process(input = in, output = s"${DSTaskConfig.io_accumulated_AB}", jobname = s"${DSTaskConfig.jobname}-preprocess")

  }

  def postprocess() = {

    { /* */
      FilterSortDT[CT2red[String, String],String, String](_.n11)
      /* */
    }.process(input = DSTaskConfig.io_dt, output = DSTaskConfig.io_dt_sorted)

  }

  var config = DSTaskConfig.resolveConfig(args)
  if(!config.hasPath("dt.jobname"))
    config = DSTaskConfig.resolveConfig(args ++ Array("-dt.jobname", getClass.getSimpleName.replaceAllLiterally("$","")))
  DSTaskConfig.load(config)

  def extractorfun:String => TraversableOnce[CT2red[String,String]] = Util.getExtractorfunFromJobname()

  // get input data
  val in = DSTaskConfig.io_text

  val preprocess_output_path:Path = new Path(DSTaskConfig.io_accumulated_AB)
  if(!preprocess_output_path.getFileSystem.exists(preprocess_output_path))
    preprocess()

  process()
  postprocess()

}
