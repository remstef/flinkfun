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

package de.tudarmstadt.lt.flinkdt.examples

import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.textutils.TextToCT2
import de.tudarmstadt.lt.flinkdt.types.{CT2def, CT2red}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object SamplePipeline extends App {

  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val ds = {
      /*  */
      Extractor(TextToCT2.ngrams(_,3), inputcolumn = DSTaskConfig.in_text_column) ~> DSWriter(DSTaskConfig.out_raw) ~>
      /*  */
      N11Sum[CT2red[String,String], String,String]() ~> DSWriter(DSTaskConfig.out_accumulated_AB) ~>
      /*  */
      WhiteListFilter[CT2red[String,String],String, String](DSTaskConfig.in_whitelist) ~|~>
      /*  */
      ComputeCT2[CT2red[String,String], CT2def[String,String], String,String](prune = true, sigfun = _.lmi, order = Order.DESCENDING) ~> DSWriter(DSTaskConfig.out_accumulated_CT) ~>
      /*  */
      ComputeDTSimplified.byJoin[CT2def[String,String],String,String]() ~>
      /*  */
      FilterSortDT[CT2red[String,String],String, String](_.n11)
      /*  */
  }.process(input = in, output = DSTaskConfig.out_dt_sorted)


}
