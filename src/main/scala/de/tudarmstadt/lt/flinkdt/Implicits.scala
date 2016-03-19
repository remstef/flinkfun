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
import de.tudarmstadt.lt.flinkdt.textutils.{CtFromString, StringConvert}
import de.tudarmstadt.lt.flinkdt.types.{CT2def, CT2ext, CT2red, CT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import scala.reflect.ClassTag
import org.apache.flink.api.scala._
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.api.common.io.FileInputFormat

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
    def readCT2r(in: String): DataSet[CT2red[String,String]] =
      env.readTextFile(in).map(CtFromString[CT2red[String,String], String, String](_))
    def readCT2d(in: String): DataSet[CT2def[String,String]] =
      env.readTextFile(in).map(CtFromString[CT2def[String,String], String, String](_))
    def readCT2e(in: String): DataSet[CT2ext[String,String]] =
      env.readTextFile(in).map(CtFromString[CT2ext[String,String], String, String](_))
  }


  implicit def ct2_ext_computation(x: DataSet[CT2red[String,String]]) = new {
    def computeCT2ext:DataSet[CT2ext[String,String]] =
      ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]().process(x)
  }

  implicit def dt_computation(x: DataSet[CT2ext[String,String]]) = new {
    def computeDT(prune: Boolean = false):DataSet[CT2red[String,String]] = {
      val p:DSTask[CT2ext[String,String], CT2red[String,String]] = ComputeDTSimplified.byJoin[CT2ext[String, String], String, String]()
      if(prune)
        { Prune[String, String](sigfun = _.lmi_n, Order.ASCENDING) ~> p }.process(x)
      else
        p.process(x)
    }
  }

  implicit def ct2_get_top[CT <: CT2 : ClassTag : TypeInformation](x: DataSet[CT]) = new {
    def topN(n:Int, valfun:CT => Float = _.n11, order:Order = Order.DESCENDING):DataSet[CT] = {
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

  implicit def checkpointed[T : ClassTag : TypeInformation](ds: DataSet[T]) = new {
    def checkpointed(location:String, toStringFun:T => String, fromStringFun:String => T, jobname:String, reReadFromCheckpoint:Boolean)(implicit env:ExecutionEnvironment):DataSet[T] = {
      val output_path:Path = new Path(location)
      if(output_path.getFileSystem.exists(output_path))
        return DSReader(location, env).process().map(fromStringFun)

      if(location != null) {
        DSWriter[String](location, jobname).process(ds.map(toStringFun(_)))
        if(reReadFromCheckpoint) {
          // throw away intermediate results and continue to work with the re-read data
          ds.getExecutionEnvironment.startNewSession()
          return DSReader(location, env).process().map(fromStringFun)
        }
        // else just re-use the processed data
      }
      return ds
    }
  }
  
//  implicit def checkpointed_with_InputOutputFormat[T : ClassTag : TypeInformation](ds: DataSet[T]) = new {
//    def checkpointed(location:String, jobname:String, reReadFromCheckpoint:Boolean)(implicit env:ExecutionEnvironment, fof:FileOutputFormat[T], fif:FileInputFormat[T]):DataSet[T] = {
//      val output_path:Path = new Path(location)
//      if(output_path.getFileSystem.exists(output_path))
//        return DSReader[T](location, fif, env).process()
//
//      if(location != null) {
//        DSWriter[T](location, jobname, fof).process(ds)
//        if(reReadFromCheckpoint) {
//          // throw away intermediate results and continue to work with the re-read data
//          ds.getExecutionEnvironment.startNewSession()
//          return DSReader[T](location, fif, env).process()
//        }
//        // else just re-use the processed data
//      }
//      return ds
//    }
//  }

  implicit def applyTask[I : ClassTag : TypeInformation](ds: DataSet[I]) = new {
    def applyTask[O : ClassTag : TypeInformation](dsTask: DSTask[I, O]): DataSet[O] = dsTask(ds)
  }

}
