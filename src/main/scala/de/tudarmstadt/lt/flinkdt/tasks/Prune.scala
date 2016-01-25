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

package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object Prune {

  def apply[C <: CT2ext : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](sigfun:C => Float, order:Order = Order.DESCENDING) = new Prune[C, T1, T2](sigfun, order)

}


class Prune[C <: CT2ext : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](sigfun:(C => Float), order:Order) extends DSTask[C, C] {

  override def fromLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C,T1,T2](_))

  override def process(ds: DataSet[C]): DataSet[C] = {

    var dsf = ds
      .filter(_.n11 >= DSTaskConfig.param_min_n11)
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)
      .filter(ct => ct.odot1 <= DSTaskConfig.param_max_odot1 && ct.odot1 >= DSTaskConfig.param_min_odot1)

    dsf = dsf
      .map(ct => (ct, sigfun(ct)))
      .filter(_._2 >= DSTaskConfig.param_min_sig)
      .groupBy("_1.a")
      .sortGroup("_2", order)
      .first(DSTaskConfig.param_topn_sig)
      .map(_._1)

    dsf
  }

}

