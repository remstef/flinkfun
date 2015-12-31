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
import de.tudarmstadt.lt.flinkdt.types.{CT2Min, CT2}
import de.tudarmstadt.lt.flinkdt.{Util, TextToCT2}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object FullDT extends App {

  def process[T : ClassTag : TypeInformation]() = {
    { /* */
//      ComputeDTSimplified.CT2MinGraph[T,T]() ~> DSWriter(DSTaskConfig.out_dt)
      new DSTask[CT2[T, T], CT2[T,T]] {
        override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T, T]] = lineDS.map(CT2.fromString(_))
        override def process(ds: DataSet[CT2[T, T]]): DataSet[CT2[T, T]] = {
          val dsf = ds.filter(_.ndot1 > 1)
          dsf.map((_,1)).groupBy("_1.b").sum(1).filter(_._2 > 1).map(_._1)
        }
      }
      ComputeDTSimplified.CT2Join[T,T]() ~> DSWriter(DSTaskConfig.out_dt)
      /* */
    }.process(env, input = s"${DSTaskConfig.out_accumulated_CT}")

    env.execute(s"${DSTaskConfig.jobname}-process")

  }

  def preprocess(hash:Boolean = false) = {

    val string_preprocessing_chain =
      { /* */
        Extractor(extractorfun, inputcolumn = DSTaskConfig.in_text_column) ~|~>
        /*  */
//        N11Sum.toCT2Min[String, String]()
        ComputeCT2.fromCT2Min()
      }

    val preprocessing_chain =
      if(hash) { string_preprocessing_chain ~> Convert.HashCT2Types.StringSha256(DSTaskConfig.out_keymap) }
      else string_preprocessing_chain

    preprocessing_chain.process(env, input = in, output = DSTaskConfig.out_accumulated_CT)

    env.execute(s"${DSTaskConfig.jobname}-preprocess")
    env.startNewSession()

  }

  def postprocess(hash:Boolean = false) = {
    env.startNewSession()

    val sting_post_processing = FilterSortDT.CT2Min[String, String]()

    val postprocessing_chain =
      if(hash){ Convert.HashCT2MinTypes.Reverse[String, String](env, DSTaskConfig.out_keymap) ~> sting_post_processing }
      else sting_post_processing

    postprocessing_chain.process(env, input = DSTaskConfig.out_dt, output = DSTaskConfig.out_dt_sorted)

    env.execute(s"${DSTaskConfig.jobname}-postprocess")

  }

  DSTaskConfig.load(args, getClass.getSimpleName.replaceAllLiterally("$",""))

  def extractorfun:String => TraversableOnce[CT2Min[String,String]] = Util.getExtractorfunFromJobname()

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text
  val hash = DSTaskConfig.jobname.toLowerCase.contains("hash")

  val preprocess_output_path:Path = new Path(DSTaskConfig.out_accumulated_AB)
  if(!preprocess_output_path.getFileSystem.exists(preprocess_output_path))
    preprocess(hash)

  if(hash)
    process[Array[Byte]]()
  else
    process[String]()

  postprocess(hash)

}
