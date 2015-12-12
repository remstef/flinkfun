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


import de.tudarmstadt.lt.flinkdt.{CT2, CT2Min}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object NSum {

  def CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](broadcastName:String = "NSum", env:ExecutionEnvironment = null):NSum__CT2Min[T1,T2] = new NSum__CT2Min[T1,T2](broadcastName, env)

  def CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]():NSum__CT2[T1,T2] = new NSum__CT2[T1,T2]()

}

case class N(val n:Float, val n_distinct:Float)

class NSum__CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](broadcastName:String = "NSum", env:ExecutionEnvironment = null) extends DSTask[CT2Min[T1,T2], CT2Min[T1,T2]]{

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString[T1,T2](_))

  override def process(ds: DataSet[CT2Min[T1, T2]]): DataSet[CT2Min[T1, T2]] = {
    val ds_sum = ds
      .map(ct => N(ct.n11, 1f))
      .reduce((l,r) => N(l.n + r.n, l.n_distinct + r.n_distinct))
    return ds
  }

}

class NSum__CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2[T1,T2], CT2[T1,T2]]{

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1, T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  override def process(ds: DataSet[CT2[T1, T2]]): DataSet[CT2[T1, T2]] = {
    ???
  }

}
