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

import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.{CT2Min, TextToCT2}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object FullHash extends App {

  def process[T : ClassTag : TypeInformation]() = {

    { /* */
      ComputeDTSimplified.CT2MinGraph[T,T]() ~> DSWriter(DSTaskConfig.out_dt)
      /* */
    }.process(env, input = s"${DSTaskConfig.out_accumulated_AB}")

    env.execute(s"${DSTaskConfig.jobname}-process")

  }

  def preprocess() = {

    { /* */
      Extractor(extractorfun) ~|~>
      /*  */
      N11Sum.toCT2Min[String, String]() ~|~>
      /*  */
      Convert.HashCT2MinTypes.StringSha256(DSTaskConfig.out_keymap)
      /*  */
    }.process(env, input = in, output = s"${DSTaskConfig.out_accumulated_AB}")

    env.execute(s"${DSTaskConfig.jobname}-preprocess")
    env.startNewSession()

  }

  def postprocess() = {

    env.startNewSession()

    {
      Convert.HashCT2MinTypes.Reverse[String, String](env, DSTaskConfig.out_keymap) ~>
      /* */
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

  process[Array[Byte]]()

  postprocess()



}
