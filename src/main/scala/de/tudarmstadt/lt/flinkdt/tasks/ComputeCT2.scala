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

package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.types.{CT2Min, CT2}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object ComputeCT2 {

  def fromCT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new From_CT2Min[T1,T2]()

  def fromCT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new From_CT2withPartialN[T1,T2]()

}

class From_CT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2[T1,T2], CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  override def process(cts: DataSet[CT2[T1,T2]]): DataSet[CT2[T1,T2]] = {

    //    cts.filter(_.a == "banana").map(_.prettyPrint()).print() // pretty-print for debugging purposes

    val ct_accumulated_A = cts
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l})

    val ct_accumulated_B = cts
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l})

    val cts_j1 = cts
      .leftOuterJoin(ct_accumulated_A)
      .where("a").equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })

    val cts_j_all = cts_j1
      .leftOuterJoin(ct_accumulated_B)
      .where("b").equalTo("b")((x, y) => { x.ndot1 = y.ndot1; x })

    //    ct_all.filter(_._1.a == "banana").map(_._1.prettyPrint()).print() // pretty-print for debugging purposes

    cts_j_all
  }

}


/**
  *
  * Needs to recompute N from what is provided!
  *
  * @tparam T1
  * @tparam T2
  */
class From_CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2Min[T1,T2], CT2[T1,T2]] {

  @transient
  val chain:DSTask[CT2Min[T1,T2], CT2[T1,T2]] = N11Sum.toCT2withN[T1,T2]() ~>  ComputeCT2.fromCT2withPartialN[T1,T2]()

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(l => CT2Min.fromString[T1,T2](l))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2[T1,T2]] = {
    chain(ds)
  }

}
