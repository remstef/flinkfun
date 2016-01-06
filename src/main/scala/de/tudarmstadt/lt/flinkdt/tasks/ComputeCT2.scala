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

import de.tudarmstadt.lt.flinkdt.types._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object ComputeCT2 {

  def apply[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeCT2[C,T1,T2]()

  def fromCT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = { N11Sum.toCT2withN[T1,T2] ~>  fromCT2withPartialN[T1,T2] }

  def fromCT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new From_CT2withPartialN[T1,T2]()

}

class ComputeCT2[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[C,C] {

  override def fromLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C,T1,T2](_))

  override def process(ds: DataSet[C]): DataSet[C] = {
    null
  }

  def process_CT2red(ds: DataSet[C]) : DataSet[C] = {
    ds.groupBy("a","b")
      //      .reduce((l,r) => {l.n11 += r.n11; l}) // .sum("n11")
      .sum("n11")
  }

  def process_complete_CT2def(ds: DataSet[CT2def[T1,T2]]) : DataSet[CT2def[T1,T2]] = {

    val ct_accumulated_A = ds
      .groupBy("a")
      .sum("n1dot")

    val ct_accumulated_B = ds
      .groupBy("b")
      .sum("ndot1")

    val cts_joined = ds
      .join(ct_accumulated_A)
      .where("a").equalTo("a"){(l, r) => { l.n1dot = r.n1dot; l }}
      .join(ct_accumulated_B)
      .where("b").equalTo("b"){(l, r) => { l.ndot1 = r.ndot1; l }}

    cts_joined

  }

  def process_complete_CT2ext(ds: DataSet[CT2ext[T1,T2]]) : DataSet[CT2ext[T1,T2]] = {

    val ct_accumulated_AB = ds // process_CT2red(ds.asInstanceOf[DataSet[C]]).asInstanceOf[DataSet[CT2ext[T1,T2]]]

    val ct_accumulated_A = ct_accumulated_AB
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l.o1dot += r.o1dot; l}) // .sum("n1dot, o1dot")

    val ct_accumulated_B = ct_accumulated_AB
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.odot1 += r.odot1; l}) // .sum("ndot1, odot1")

    val cts_joined = ct_accumulated_AB
      .join(ct_accumulated_A)
      .where("a").equalTo("a"){(l, r) => { l.n1dot = r.n1dot; l.o1dot = r.o1dot; l }}
      .join(ct_accumulated_B)
      .where("b").equalTo("b"){(l, r) => { l.ndot1 = r.ndot1; l.odot1 = r.odot1; l }}

    cts_joined

  }

}

class From_CT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2def[T1,T2], CT2def[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2def[T1,T2]] = lineDS.map(CtFromString[CT2def[T1,T2],T1,T2](_))

  override def process(cts: DataSet[CT2def[T1,T2]]): DataSet[CT2def[T1,T2]] = {

    //    cts.filter(_.a == "banana").map(_.prettyPrint()).print() // pretty-print for debugging purposes

    val ct_accumulated_A = cts
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l})

    val ct_accumulated_B = cts
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l})

    val cts_joined = cts
      .join(ct_accumulated_A)
      .where("a").equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
      .join(ct_accumulated_B)
      .where("b").equalTo("b")((x, y) => { x.ndot1 = y.ndot1; x })

    //    ct_all.filter(_._1.a == "banana").map(_._1.prettyPrint()).print() // pretty-print for debugging purposes

    cts_joined
  }

}
