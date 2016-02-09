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
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import scala.reflect._

/**
  * Created by Steffen Remus.
  */
object ComputeCT2 {

  def apply[CIN <: CT2 : ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](prune:Boolean = false,sigfun:COUT => Float = null, order:Order = Order.ASCENDING) = new ComputeCT2[CIN, COUT,T1,T2](prune,sigfun,order)

}

class ComputeCT2[CIN <: CT2 : ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](prune:Boolean = false, sigfun:COUT => Float = null, order:Order = Order.ASCENDING) extends DSTask[CIN,COUT] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CIN] = lineDS.map(CtFromString[CIN,T1,T2](_))

  override def process(ds: DataSet[CIN]): DataSet[COUT] = classTag[CIN] match {
    case t if t == classTag[CT2red[T1,T2]] => process_CT2red(ds.asInstanceOf[DataSet[CT2red[T1,T2]]])
    case t if t == classTag[CT2def[T1,T2]] => process_CT2def(ds.asInstanceOf[DataSet[CT2def[T1,T2]]])
    case t if t == classTag[CT2ext[T1,T2]] => process_CT2ext(ds.asInstanceOf[DataSet[CT2ext[T1,T2]]])
  }

  def process_CT2red(ds: DataSet[CT2red[T1,T2]]) : DataSet[COUT] = classTag[COUT] match {
    case t if t == classTag[CT2red[T1,T2]] && !prune  => ds.groupBy("a","b").sum("n11").asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2red[T1,T2]] && prune  => ??? // TODO: filter//ds.groupBy("a","b").sum("n11").asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2def[T1,T2]] && !prune => process_CT2def_complete(ds.map(_.asCT2def())).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2def[T1,T2]] && prune => process_CT2def_pruned(ds.map(_.asCT2def())).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2ext[T1,T2]] && !prune => process_CT2ext__complete(ds.map(_.asCT2ext())).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2ext[T1,T2]] && prune => process_CT2ext__pruned(ds.map(_.asCT2ext())).asInstanceOf[DataSet[COUT]]
  }

  def process_CT2def(ds: DataSet[CT2def[T1,T2]]) : DataSet[COUT] = classTag[COUT] match {
    case t if t == classTag[CT2red[T1,T2]] => ???
    case t if t == classTag[CT2def[T1,T2]] && !prune => process_CT2def_complete(ds).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2def[T1,T2]] && prune => process_CT2def_pruned(ds).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2ext[T1,T2]] && !prune => process_CT2ext__complete(ds.map(_.asCT2ext())).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2ext[T1,T2]] && prune => process_CT2ext__pruned(ds.map(_.asCT2ext())).asInstanceOf[DataSet[COUT]]
  }

  def process_CT2ext(ds: DataSet[CT2ext[T1,T2]]) : DataSet[COUT] = classTag[COUT] match {
    case t if t == classTag[CT2red[T1,T2]] => ???
    case t if t == classTag[CT2def[T1,T2]] => ???
    case t if t == classTag[CT2ext[T1,T2]] && !prune => process_CT2ext__complete(ds).asInstanceOf[DataSet[COUT]]
    case t if t == classTag[CT2ext[T1,T2]] && prune => process_CT2ext__pruned(ds).asInstanceOf[DataSet[COUT]]
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


  def process_CT2def_pruned(ds: DataSet[CT2def[T1,T2]]) : DataSet[CT2def[T1,T2]] = {

    val n11:DataSet[CT2def[T1,T2]] = ds
      .groupBy("a","b")
      .sum("n11")

    val n = n11.map(ct => {ct.n = ct.n11; ct}).reduce((l,r) => {l.n += r.n; l}) //.sum("n")

    val n1dot = n11
      .map(ct => {ct.n1dot = ct.n11; ct})
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l}) // .sum("n1dot")
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)

    val ndot1 = n11
      .map(ct => {ct.ndot1 = ct.n11; ct.n = 1f; ct}) // misuse n as odot1
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.n += r.n; l}) // .sum("ndot1")
      .filter(ct => ct.n <= DSTaskConfig.param_max_odot1 && ct.n >= DSTaskConfig.param_min_odot1)

    var joined = n11
      .filter(_.n11 >= DSTaskConfig.param_min_n11)
      .join(n1dot)
      .where("a").equalTo("a"){(l, r) => { l.n1dot = r.n1dot; l }}
      .join(ndot1)
      .where("b").equalTo("b"){(l, r) => { l.ndot1 = r.ndot1; l }}

    joined = joined
      .crossWithTiny(n){(ct,n) => {ct.n = n.n; ct}}.withForwardedFieldsFirst("n11; n1dot; ndot1").withForwardedFieldsSecond("n")

    joined = joined
      .map(ct => (ct, sigfun(ct.asInstanceOf[COUT])))
      .filter(_._2 >= DSTaskConfig.param_min_sig)
      .groupBy("_1.a")
      .sortGroup("_2", order)
      .first(DSTaskConfig.param_topn_sig)
      .map(_._1)

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


  def process_CT2ext__pruned(ds: DataSet[CT2ext[T1,T2]]) : DataSet[CT2ext[T1,T2]] = {

    val n11:DataSet[CT2ext[T1,T2]] = ds
      .groupBy("a","b")
      .sum("n11")

    val n = n11.map(ct => {ct.n = ct.n11; ct}).reduce((l,r) => {l.n += r.n; l}) //.sum("n")

    val n1dot = n11
      .map(ct => {ct.n1dot = ct.n11; ct.o1dot = 1f; ct})
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l.o1dot += r.o1dot; l}) // .sum("n1dot, o1dot")
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)

    val ndot1 = n11
      .map(ct => {ct.ndot1 = ct.n11; ct.odot1 = 1f; ct})
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.odot1 += r.odot1; l}) // .sum("ndot1, odot1")
      .filter(ct => ct.odot1 <= DSTaskConfig.param_max_odot1 && ct.odot1 >= DSTaskConfig.param_min_odot1)

    var joined = n11
      .filter(_.n11 >= DSTaskConfig.param_min_n11)
      .join(n1dot)
      .where("a").equalTo("a"){(l, r) => { l.n1dot = r.n1dot; l.o1dot = r.o1dot; l }}
      .join(ndot1)
      .where("b").equalTo("b"){(l, r) => { l.ndot1 = r.ndot1; l.odot1 = r.odot1; l }}

    joined = joined
      .crossWithTiny(n){(ct,n) => {ct.n = n.n; ct.on = n.on; ct}}.withForwardedFieldsFirst("n11; n1dot; ndot1; o1dot; odot1").withForwardedFieldsSecond("n; on")

    joined = joined
      .map(ct => (ct, sigfun(ct.asInstanceOf[COUT])))
      .filter(_._2 >= DSTaskConfig.param_min_sig)
      .groupBy("_1.a")
      .sortGroup("_2", order)
      .first(DSTaskConfig.param_topn_sig)
      .map(_._1)

    joined

  }

}

