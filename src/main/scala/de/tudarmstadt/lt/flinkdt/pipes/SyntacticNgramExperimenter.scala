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
import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2def, CT2red}
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  *
  * // temp/flink-0.10.1-hadoop26-scala_2.11/bin/flink run -m yarn-cluster -yn 192 -ys 1 -ytm 2048 -yqu shortrunning -c de.tudarmstadt.lt.flinkdt.pipes.SyntacticNgramExperimenter target/flinkdt-0.1.jar googlesyntactics-app.conf
  *
  */
object SyntacticNgramExperimenter extends App {

  DSTaskConfig.load(args)

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val default_jobimtext_pipeline = {
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
  }

   val full_dt_pipeline = {
     /*  */
     ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]() ~>
       DSWriter(DSTaskConfig.out_accumulated_CT) ~>
     /* */
     DSTask(
       CtFromString[CT2ext[String,String],String,String](_),
       ds => { ds.filter(_.ndot1 > 1).filter(_.odot1 > 1) }
     ) ~>
     /* */
     ComputeDTSimplified.byJoin[CT2ext[String,String],String,String]() ~>
       DSWriter(DSTaskConfig.out_dt) ~>
     /* */
     FilterSortDT.apply[CT2red[String, String], String, String](_.n11)
   }

  if(args.contains("full"))
    full_dt_pipeline.process(env = env, input = in, output = DSTaskConfig.out_dt_sorted)
  else
    default_jobimtext_pipeline.process(env = env, input = in, output = DSTaskConfig.out_dt_sorted)

  env.execute(DSTaskConfig.jobname)

}
