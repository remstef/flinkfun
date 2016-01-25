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

import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.types.{CT2def, CT2red}
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object SyntacticNgramExperimenter extends App {

  DSTaskConfig.load(args)

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val ds = {
      /*  */
      N11Sum.toCT2withN[String,String]() ~>
        DSWriter(DSTaskConfig.out_accumulated_AB) ~>
      /*  */
      ComputeSignificanceFiltered.fromCT2withPartialN[String,String](sigfun = _.lmi) ~>
        DSWriter(DSTaskConfig.out_accumulated_CT) ~>
      /*  */
      ComputeDTSimplified.byJoin[CT2def[String,String],String,String]() ~>
        DSWriter(DSTaskConfig.out_dt) ~>
      /*  */
      FilterSortDT[CT2red[String,String],String, String](_.n11)
    /*  */
  }.process(env = env, input = in, output = DSTaskConfig.out_dt_sorted)

  env.execute(DSTaskConfig.jobname)


}
