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

package de.tudarmstadt.lt.flinkdt

import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.textutils.{StringConvert}
import de.tudarmstadt.lt.flinkdt.types.{CT2def, CT2ext, CT2red, CT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import scala.reflect.ClassTag
import org.apache.flink.api.scala._
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.api.common.io.FileInputFormat
import scala.collection.mutable

/**
  * Created by Steffen Remus.
  */
object Implicits {

  implicit def string_conversion(x: String) = new {
    def toT[T : ClassTag]:T = StringConvert.convert_toType(x)
  }

  implicit def string_conversion(x: Any) = new {
    def asString:String = StringConvert.convert_toString(x)
  }

  ///
  // convenience: define some implicit functions on DataSet[CT2] objects
  ///

  implicit def read_ct2(env: ExecutionEnvironment) = new {
    def readCT2r(in: String, includedFields:Array[Int]=Array(0,1,2)): DataSet[CT2red[String,String]] = {
      if(includedFields.length < 3)
        env.readCsvFile[(String,String)](DSTaskConfig.io_ctraw, fieldDelimiter="\t", includedFields=includedFields).map(t => CT2red[String,String](t._1, t._2))
      else
        env.readCsvFile[CT2red[String,String]](DSTaskConfig.io_ctraw, fieldDelimiter="\t", includedFields=includedFields)
    }
    def readCT2d(in: String): DataSet[CT2def[String,String]] =
      env.readCsvFile[CT2def[String,String]](in, fieldDelimiter="\t")
    def readCT2e(in: String): DataSet[CT2ext[String,String]] =
      env.readCsvFile[CT2ext[String,String]](in, fieldDelimiter="\t")
  }


  implicit def ct2_ext_computation(x: DataSet[CT2red[String,String]]) = new {
    def computeCT2ext:DataSet[CT2ext[String,String]] =
      ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]().process(x)
  }

  implicit def dt_computation(x: DataSet[CT2ext[String,String]]) = new {
    def computeDT(prune: Boolean = false):DataSet[CT2red[String,String]] = {
      val p:DSTask[CT2ext[String,String], CT2red[String,String]] = ComputeDTSimplified.byJoin[CT2ext[String, String], String, String]()
      if(prune)
        { Prune[String, String](sigfun = _.lmi_n, Order.DESCENDING) ~> p }.process(x)
      else
        p.process(x)
    }
  }

  implicit def ct2_get_top[CT <: CT2 : ClassTag : TypeInformation](x: DataSet[CT]) = new {
    def topN(n:Int, valfun:CT => Double = _.n11, order:Order = Order.DESCENDING):DataSet[CT] = {
      val ds_first_n = x
        .map(ct => (ct, valfun(ct))) // apply valfun
        .groupBy("_1.a")
        .sortGroup(1, order)
        .first(n)
        .map(_._1)
      ds_first_n
    }
  }

  implicit def prettyprint_ct2[CT <: CT2 : ClassTag : TypeInformation](x: DataSet[CT]) = new {
    def prettyprint = x.map(_.prettyprint).print
  }

  implicit def writeAsGraph(x: DataSet[CT2]) = new {
    def writeAsGraph(out:String):DataSet[CT2] = {
      GraphWriter[CT2, String, String](out).process(x)
    }
  }

  implicit def writeCT(x: DataSet[CT2]) = new {
    def writeCt(out:String):DataSet[CT2] = {
      DSWriter[CT2](out, "shell-job").process(x)
    }
  }

  implicit def generalfun[T : ClassTag : TypeInformation](ds: DataSet[T]) = new {
    
//    def checkpointed(location:String, jobname:String, reReadFromCheckpoint:Boolean, env:ExecutionEnvironment):DataSet[T] = {
//      checkpointed(location, jobname, reReadFromCheckpoint, env)(null, null)
//    }
    
    def checkpointed(location:String, jobname:String, reReadFromCheckpoint:Boolean, env:ExecutionEnvironment)(implicit pipeline:mutable.ListBuffer[String] = mutable.ListBuffer("default"), provides:String = "default"):DataSet[T] = {
      val output_path:Path = new Path(location)
      if(output_path.getFileSystem.exists(output_path)){
        if(pipeline != null && !pipeline.isEmpty && provides == pipeline.head)
          pipeline.remove(0)
        return DSReader[T](location, env).process()
      }

      // only if location is not null and the the next step provides matches the next in the pipeline 
      println(s"PIPELINE: $pipeline; PROVIDE: $provides")
      if(location != null && ( pipeline != null && !pipeline.isEmpty && provides == pipeline.head ) ) {
        pipeline.remove(0)
        DSWriter[T](location, jobname).process(ds)
        if(reReadFromCheckpoint) {
          // throw away intermediate results and continue to work with the re-read data
          ds.getExecutionEnvironment.startNewSession()
          return DSReader[T](location, env).process()
        }
        // else just re-use the processed data
      }
      return ds
    }
    
    
    def save(location:String, jobname:String = null):Unit = {
      val output_path:Path = new Path(location)
      if(output_path.getFileSystem.exists(output_path))
          return
      if(location != null)
        DSWriter[T](location, jobname).process(ds)
    }
        
    def applyTask[O : ClassTag : TypeInformation](dsTask: DSTask[T, O]): DataSet[O] = dsTask(ds)
    
  }
  

  

}
