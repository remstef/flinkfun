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

import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object Executor extends App {

  val jobname = getClass.getSimpleName.replaceAllLiterally("$","")
  DSTaskConfig.load(args, jobname=jobname)

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val ds = {
      //
      Extractor() ~>
      //
      N11Sum() ~|~>
      //
      WhiteListFilter(DSTaskConfig.in_whitelist, env) ~|~>
      //
      ComputeCT2() ~> DSWriter(DSTaskConfig.out_accumulated_CT) ~>
      //
      ComputeDT.fromCT2() ~>
      //
      FilterSortDT.CT2Min_CT2()
    //
  }.process(env,in)


  env.execute(jobname)

}
