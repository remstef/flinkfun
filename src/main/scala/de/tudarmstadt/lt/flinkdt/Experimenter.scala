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

package de.tudarmstadt.lt.flinkdt

import de.tudarmstadt.lt.flinkdt.tasks._
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

/**
  * Created by Steffen Remus
  */
object Experimenter extends App {

  DSTaskConfig.load(args, "experimental")

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  def preprocess() = {
    { Extractor(s => TextToCT2.ngrams(s, 3)) ~|~>
      /*  */
      N11Sum.toCT2withN[String, String]()
    }.process(env, in, DSTaskConfig.out_accumulated_AB)
    env.execute(s"${DSTaskConfig.jobname}-preprocess")
    env.startNewSession()
  }

  val preprocess_output_path:Path = new Path(DSTaskConfig.out_accumulated_AB)
  if(!preprocess_output_path.getFileSystem.exists(preprocess_output_path))
    preprocess

  {
//    WhiteListFilter.CT2Min[String](DSTaskConfig.in_whitelist, env) ~|~>
    /* */
    ComputeGraphDT.freq[String,String]() ~> DSWriter(DSTaskConfig.out_dt) //~>
    /* */
//    FilterSortDT.CT2Min[String,String]() ~> DSWriter[CT2Min[String,String]](DSTaskConfig.out_dt_sorted)
    /* */
  }.process(env, input = DSTaskConfig.out_accumulated_AB)




  env.execute(DSTaskConfig.jobname)

}
