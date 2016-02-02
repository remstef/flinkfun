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

import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import scala.reflect._

/**
  * Created by Steffen Remus.
  */
object ComputeCT2 {

  def apply[CIN <: CT2 : ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeCT2[CIN, COUT,T1,T2]()

  def fromCT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = { N11Sum.toCT2withN[T1,T2] ~>  fromCT2withPartialN[T1,T2] }

  def fromCT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new From_CT2withPartialN[T1,T2]()

}

class ComputeCT2[CIN <: CT2 : ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CIN,COUT] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CIN] = lineDS.map(CtFromString[CIN,T1,T2](_))

  override def process(ds: DataSet[CIN]): DataSet[COUT] = classTag[CIN] match {
    case t if t == classTag[CT2red[T1,T2]] => process_CT2red(ds.asInstanceOf[DataSet[CT2red[T1,T2]]])
    case t if t == classTag[CT2def[T1,T2]] => process_CT2def(ds.asInstanceOf[DataSet[CT2def[T1,T2]]])
    case t if t == classTag[CT2ext[T1,T2]] => ???
  }

  def process_CT2red(ds: DataSet[CT2red[T1,T2]]) : DataSet[COUT] = classTag[COUT] match {
    case t if t == classTag[CT2red[T1,T2]] => ds.groupBy("a","b").sum("n11").asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2def[T1,T2]] => process_CT2def_complete(ds.map(_.asCT2def())).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2ext[T1,T2]] => process_CT2ext__complete(ds.map(_.asCT2ext())).asInstanceOf[DataSet[COUT]]
  }


  def process_CT2def(ds: DataSet[CT2def[T1,T2]]) : DataSet[COUT] = classTag[COUT] match {
    case t if t == classTag[CT2red[T1,T2]] => ???
    case t if t == classTag[CT2def[T1,T2]] => process_CT2def_complete(ds).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2ext[T1,T2]] => process_CT2ext__complete(ds.map(_.asCT2ext())).asInstanceOf[DataSet[COUT]]
  }


  def process_CT2def_complete(ds: DataSet[CT2def[T1,T2]]) : DataSet[CT2def[T1,T2]] = {

    val n11 = ds
      .groupBy("a","b")
      .sum("n11")

    val n = n11.map(ct => {ct.n = ct.n11; ct}).reduce((l,r) => {l.n += r.n; l}) //.sum("n")

    val n1dot = n11
      .map(ct => {ct.n1dot = ct.n11; ct})
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l}) // .sum("n1dot")

    val ndot1 = n11
      .map(ct => {ct.ndot1 = ct.n11; ct})
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l}) // .sum("ndot1")

    var joined = n11
      .join(n1dot)
      .where("a").equalTo("a"){(l, r) => { l.n1dot = r.n1dot; l }}
      .join(ndot1)
      .where("b").equalTo("b"){(l, r) => { l.ndot1 = r.ndot1; l }}

    joined = joined
      .crossWithTiny(n){(ct,n) => {ct.n = n.n; ct}}.withForwardedFieldsFirst("n11; n1dot; ndot1").withForwardedFieldsSecond("n")

    joined

  }

  def process_CT2ext__complete(ds: DataSet[CT2ext[T1,T2]]) : DataSet[CT2ext[T1,T2]] = {

    val n11 = ds
      .groupBy("a","b")
      .sum("n11")

    val n = n11.map(ct => {ct.n = ct.n11; ct.on = 1f; ct}).reduce((l,r) => {l.n += r.n; l.on += r.on; l})

    val n1dot = n11
      .map(ct => {ct.n1dot = ct.n11; ct.o1dot = 1f; ct})
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l.o1dot += r.o1dot; l}) // .sum("n1dot, o1dot")

    val ndot1 = n11
      .map(ct => {ct.ndot1 = ct.n11; ct.odot1 = 1f; ct})
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.odot1 += r.odot1; l}) // .sum("ndot1, odot1")

    var joined = n11
      .join(n1dot)
      .where("a").equalTo("a"){(l, r) => { l.n1dot = r.n1dot; l.o1dot = r.o1dot; l }}
      .join(ndot1)
      .where("b").equalTo("b"){(l, r) => { l.ndot1 = r.ndot1; l.odot1 = r.odot1; l }}

    joined = joined
      .crossWithTiny(n){(ct,n) => {ct.n = n.n; ct.on = n.on; ct}}.withForwardedFieldsFirst("n11; n1dot; ndot1; o1dot; odot1").withForwardedFieldsSecond("n; on")

    joined

  }

}


@deprecated
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
