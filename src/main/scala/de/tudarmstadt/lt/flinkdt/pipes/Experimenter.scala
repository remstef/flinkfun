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

import de.tudarmstadt.lt.flinkdt.types.CT2Min
import de.tudarmstadt.lt.flinkdt.Util
import de.tudarmstadt.lt.flinkdt.tasks._
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

/**
  * Created by Steffen Remus
  */
object Experimenter extends App {

  def process() = {

    { /* */
      //WhiteListFilter.CT2Min[String, String](DSTaskConfig.in_whitelist, env) ~|~>
      /* */
      //      ComputeDTSimplified.CT2MinJoin[String,String]() ~> DSWriter(DSTaskConfig.out_dt)
      ComputeDTSimplified.CT2MinGraph[String,String]() ~> DSWriter(DSTaskConfig.out_dt)
      /* */
    }.process(env, input = s"${DSTaskConfig.out_accumulated_AB}")

    env.execute(s"${DSTaskConfig.jobname}-process")

  }

  def preprocess() = {

    { /* */
      Extractor(extractorfun, inputcolumn = DSTaskConfig.in_text_column) ~|~>
      /*  */
      N11Sum[CT2Min[String,String], String, String]
      /*  */
    }.process(env, input = in, output = s"${DSTaskConfig.out_accumulated_AB}")

    env.execute(s"${DSTaskConfig.jobname}-preprocess")
    env.startNewSession()

  }

  def postprocess() = {

    env.startNewSession()

    { /* */
      FilterSortDT[CT2Min[String,String], String, String](_.n11)
      /* */
    }.process(env, input = DSTaskConfig.out_dt, output = DSTaskConfig.out_dt_sorted)

    env.execute(s"${DSTaskConfig.jobname}-postprocess")

  }

  DSTaskConfig.load(args, getClass.getSimpleName.replaceAllLiterally("$",""))

  def extractorfun:String => TraversableOnce[CT2Min[String,String]] = Util.getExtractorfunFromJobname()

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

//  val preprocess_output_path:Path = new Path(DSTaskConfig.out_accumulated_AB)
//  if(!preprocess_output_path.getFileSystem.exists(preprocess_output_path))
  preprocess()

  process()

  postprocess()

}
