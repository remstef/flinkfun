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

import de.tudarmstadt.lt.flinkdt.tasks.{Prune, ComputeDTSimplified, DSTask, ComputeCT2}
import de.tudarmstadt.lt.flinkdt.textutils.{CtFromString, StringConvert}
import de.tudarmstadt.lt.flinkdt.types.{CT2def, CT2ext, CT2red, CT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.ClassTag

import org.apache.flink.api.scala._

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

}
