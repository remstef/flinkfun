/*
 * Copyright (c) 2015
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

package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2, TextToCT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object Executor extends App {

  DSTaskConfig.load(args)

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val ds = {
      /*  */
      Extractor(s => TextToCT2.ngrams(s,3)) ~> DSWriter(DSTaskConfig.out_raw) ~>
      /*  */
      N11Sum.toCT2withN[String,String]() ~> DSWriter(DSTaskConfig.out_accumulated_AB) ~>
      /*  */
      WhiteListFilter.CT2[String, String](DSTaskConfig.in_whitelist, env) ~|~>
      /*  */
      ComputeFilteredCT2s.fromCT2withPartialN[String,String]() ~> DSWriter(DSTaskConfig.out_accumulated_CT) ~>
      /*  */
      ComputeDT.fromCT2[String,String]() ~>
      /*  */
      FilterSortDT.CT2Min[String,String](_.n11)
      /*  */
  }.process(env,in, DSTaskConfig.out_dt_sorted)

  env.execute(DSTaskConfig.jobname)

}
