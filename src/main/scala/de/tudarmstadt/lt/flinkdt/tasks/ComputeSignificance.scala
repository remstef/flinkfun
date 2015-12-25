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

import de.tudarmstadt.lt.flinkdt.{CT2Min, CT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag


/**
  * Created by Steffen Remus
  */
object ComputeSignificance {

  def fromCT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new Sig__from_CT2Min[T1,T2]()

  def fromCT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new Sig_from_CT2withPartialN[T1,T2]()

}

object ComputeSignificanceFiltered {

  def fromCT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](sigfun:CT2[_,_] => Float) = new SigFilter__from_CT2Min[T1,T2](sigfun)

  def fromCT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](sigfun:CT2[_,_] => Float) = new SigFilter_from_CT2withPartialN[T1,T2](sigfun)

}

/**
  *
  *
  * @tparam T1
  * @tparam T2
  */
class Sig_from_CT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2[T1,T2],(CT2[T1,T2], Float)] {

  @transient
  val compute_ct:DSTask[CT2[T1,T2], CT2[T1,T2]] = ComputeCT2.fromCT2withPartialN[T1,T2]()

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  override def process(cts: DataSet[CT2[T1,T2]]): DataSet[(CT2[T1,T2], Float)] = {
    compute_ct(cts)
      .map(ct => (ct, ct.lmi()))
  }

}


/**
  *
  * Needs to recompute N from what is provided!
  *
  * @tparam T1
  * @tparam T2
  */
class Sig__from_CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2Min[T1,T2],(CT2[T1,T2], Float)] {

  @transient
  val chain:DSTask[CT2Min[T1,T2], (CT2[T1,T2], Float)] = N11Sum.toCT2withN[T1,T2]() ~>  ComputeSignificance.fromCT2withPartialN[T1,T2]()

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(l => CT2Min.fromString[T1,T2](l))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[(CT2[T1,T2], Float)] = {
    chain(ds)
  }

}

/**
  *
  *
  * @tparam T1
  * @tparam T2
  */
class SigFilter_from_CT2withPartialN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](sigfun:CT2[_,_] => Float) extends DSTask[CT2[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  override def process(cts: DataSet[CT2[T1,T2]]): DataSet[CT2[T1,T2]] = {

    val ctsf = cts.filter(_.n11 >= DSTaskConfig.param_min_n11)

    val ct_accumulated_A = ctsf
      .groupBy("a")
      .reduce((l,r) => {l.n1dot = l.n1dot+r.n1dot; l})
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)

    val ct_accumulated_B = ctsf
      .map(ct => {ct.n = 1f; ct}) // misuse n as odot1 i.e. the number of distinct occurrences of feature B (parameter wc=wordcount or wpfmax=wordsperfeature in traditional jobimtext)
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.n += r.n; l})
      .filter(ct => ct.n <= DSTaskConfig.param_max_odot1 && ct.n >= DSTaskConfig.param_min_odot1)

    val ct_all = ctsf
      .join(ct_accumulated_A)
      .where("a").equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
      .join(ct_accumulated_B)
      .where("b").equalTo("b")((x, y) => { x.ndot1 = y.ndot1; x })
      .map(ct => (ct, sigfun(ct)))

    val ct_all_filtered = ct_all
      .filter(_._2 >= DSTaskConfig.param_min_sig)
      .groupBy("_1.a")
      .sortGroup("_2", Order.DESCENDING)
      .first(DSTaskConfig.param_topn_f)
      .map(_._1)

    ct_all_filtered
  }

}


/**
  *
  * Needs to recompute N from what is provided!
  *
  * ATTENTION: this changes scores that depend on N like p(A), p(B), p(A,B), pmi(A,B) and lmi(A,B) and immediately influence filtering!
  *
  * @tparam T1
  * @tparam T2
  */
class SigFilter__from_CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](sigfun:CT2[_,_] => Float) extends DSTask[CT2Min[T1,T2],CT2[T1,T2]] {

  @transient
  val chain:DSTask[CT2Min[T1,T2], CT2[T1,T2]] = N11Sum.toCT2withN[T1,T2]() ~>  ComputeSignificanceFiltered.fromCT2withPartialN[T1,T2](sigfun)

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(l => CT2Min.fromString[T1,T2](l))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2[T1,T2]] = {
    chain(ds)
  }

}
