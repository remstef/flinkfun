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
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2def, CT2red}
import de.tudarmstadt.lt.flinkdt.{Util}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object FullDT extends App {

  def process[T : ClassTag : TypeInformation]() = {
    {/* */
     DSTask[CT2ext[T,T], CT2ext[T,T]](
        ds => { ds.filter(_.ndot1 > 1).filter(_.odot1 > 1) }
     ) ~>
     //      ComputeDTSimplified.CT2MinGraph[T,T]()
     ComputeDTSimplified.byJoin[CT2ext[T,T],T,T]() ~>
     DSWriter(DSTaskConfig.io_dt, s"${DSTaskConfig.jobname}-process")
     /* */
    }.process(input = s"${DSTaskConfig.io_accumulated_CT}")


  }

  def preprocess(hash:Boolean = false) = {

    val string_preprocessing_chain:DSTask[String, CT2ext[String,String]] =
      { /* */
        Extractor(extractorfun, inputcolumn = DSTaskConfig.io_text_column) ~|~>
        /*  */
//        N11Sum.toCT2Min[String, String]()
        ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]()
      }

    val preprocessing_chain =
      if(hash) { string_preprocessing_chain ~> Convert.Hash.StringSha256[CT2ext[String,String], String, String, CT2ext[Array[Byte], Array[Byte]]](DSTaskConfig.io_keymap) }
      else
        string_preprocessing_chain

    preprocessing_chain.process(input = in, output = DSTaskConfig.io_accumulated_CT, jobname = s"${DSTaskConfig.jobname}-preprocess")

  }

  def postprocess(hash:Boolean = false) = {

    val string_post_processing = FilterSortDT.apply[CT2red[String, String], String, String](_.n11)

    val postprocessing_chain =
      if(hash){ Convert.Hash.Reverse[CT2red[Array[Byte], Array[Byte]], CT2red[String,String], String, String](DSTaskConfig.io_keymap) ~> string_post_processing }
      else string_post_processing

    postprocessing_chain.process(input = DSTaskConfig.io_dt, output = DSTaskConfig.io_dt_sorted, jobname = s"${DSTaskConfig.jobname}-postprocess")

  }

  var config = DSTaskConfig.resolveConfig(args)
  if(!config.hasPath("dt.jobname"))
    config = DSTaskConfig.resolveConfig(args ++ Array("-dt.jobname", getClass.getSimpleName.replaceAllLiterally("$","")))
  DSTaskConfig.load(config)

  def extractorfun:String => TraversableOnce[CT2red[String,String]] = Util.getExtractorfunFromJobname()

  // get input data
  val in = DSTaskConfig.io_text
  val hash = DSTaskConfig.jobname.toLowerCase.contains("hash")

  val preprocess_output_path:Path = new Path(DSTaskConfig.io_accumulated_CT)
  if(!preprocess_output_path.getFileSystem.exists(preprocess_output_path))
    preprocess(hash)

  if(hash)
    process[Array[Byte]]()
  else
    process[String]()

  postprocess(hash)

}
