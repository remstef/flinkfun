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

package de.tudarmstadt.lt.flinkdt.exp

import de.tudarmstadt.lt.flinkdt.{CT2, TextToCT2, CT2Min}
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
      ComputeSignificanceFiltered.fromCT2withPartialN[String,String](sigfun = _.lmi) ~> DSWriter(DSTaskConfig.out_accumulated_CT) ~>
      /*  */
      ComputeDTSimplified.CT2Join[String,String]()
      /* */
    }.process(env, input = DSTaskConfig.out_accumulated_AB, output =  DSTaskConfig.out_dt)

    env.execute(s"${DSTaskConfig.jobname}-process")

  }

  def preprocess() = {

    { /* */
      Extractor(extractorfun) ~|~>
      /*  */
      N11Sum.toCT2withN[String, String]()
      /*  */
    }.process(env, input = in, output = s"${DSTaskConfig.out_accumulated_AB}")

    env.execute(s"${DSTaskConfig.jobname}-preprocess")
    env.startNewSession()

  }

  def postprocess() = {

    env.startNewSession()

    { /* */
      FilterSortDT.CT2Min[String, String]()
      /* */
    }.process(env, input = DSTaskConfig.out_dt, output = DSTaskConfig.out_dt_sorted)

    env.execute(s"${DSTaskConfig.jobname}-postprocess")

  }

  DSTaskConfig.load(args, if(args.length > 1) args(1) else "experimental")

  def extractorfun:String => TraversableOnce[CT2Min[String,String]] =
    if(DSTaskConfig.jobname.contains("pattern"))
      (s => TextToCT2.kWildcardNgramPatternsPlus(s, 3))
    else
      (s => TextToCT2.ngrams(s, n=3))

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val preprocess_output_path:Path = new Path(DSTaskConfig.out_accumulated_AB)
  if(!preprocess_output_path.getFileSystem.exists(preprocess_output_path))
    preprocess()

  process()
  postprocess()

}
