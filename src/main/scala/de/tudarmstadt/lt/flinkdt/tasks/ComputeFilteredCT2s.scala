package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2Min, CT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._


/**
  * Created by Steffen Remus
  */
object ComputeFilteredCT2s {

  def fromCT2Min[T1 : TypeInformation, T2 : TypeInformation]() = new Compute__CT2_from_CT2Min[T1,T2]()

  def fromCT2withPartialN[T1 : TypeInformation, T2 : TypeInformation]() = new Compute__CT2_from_CT2withPartialN[T1,T2]()

}

/**
  *
  *
  * @tparam T1
  * @tparam T2
  */
class Compute__CT2_from_CT2withPartialN[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  override def process(cts: DataSet[CT2[T1,T2]]): DataSet[CT2[T1,T2]] = {

//    cts.filter(_.a == "banana").map(_.prettyPrint()).print() // pretty-print for debugging purposes

    val ct_accumulated_A = cts
      .groupBy("a")
      .reduce((l,r) => {l.n1dot = l.n1dot+r.n1dot; l})
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)

    val ct_accumulated_B = cts
      .map(ct => {ct.n = 1f; ct}) // misuse n as odot1 i.e. the number of distinct occurrences of feature B (parameter wc=wordcount or wpfmax=wordsperfeature in traditional jobimtext)
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.n += r.n; l})
      .filter(ct => ct.n <= DSTaskConfig.param_max_odot1 && ct.n >= DSTaskConfig.param_min_odot1)

    val ct_all = cts
      .join(ct_accumulated_A)
      .where("a").equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
      .join(ct_accumulated_B)
      .where("b").equalTo("b")((x, y) => { x.ndot1 = y.ndot1; x })
      .map(ct => (ct, ct.lmi()))

//    ct_all.filter(_._1.a == "banana").map(_._1.prettyPrint()).print() // pretty-print for debugging purposes

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
class Compute__CT2_from_CT2Min[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString(_))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2[T1,T2]] = {

    // compute n before any filtering
    val n = ds.map(ct => ct.n11).reduce((l,r) => l+r)

    val cts:DataSet[CT2[T1,T2]] = ds.map(ct => CT2[T1,T2](ct.a, ct.b, n11 = ct.n11, n1dot = ct.n11, ndot1 = ct.n11, n = ct.n11))

    val ct_accumulated_A = cts
      .groupBy("a")
      .reduce((l,r) => {l.n1dot = l.n1dot+r.n1dot; l})
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)

    val ct_accumulated_B = cts
      .map(ct => {ct.n = 1f; ct}) // misuse n as odot1 i.e. the number of distinct occurrences of feature B (parameter wc=wordcount or wpfmax=wordsperfeature in traditional jobimtext)
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.n += r.n; l})
      .filter(ct => ct.n <= DSTaskConfig.param_max_odot1 && ct.n >= DSTaskConfig.param_min_odot1)

    val ct_accumulated_n = cts.crossWithTiny(n)((ct,n) => {ct.n = n; ct})

    val ct_all = ct_accumulated_n
      .join(ct_accumulated_A)
      .where("a").equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
      .join(ct_accumulated_B)
      .where("b").equalTo("b")((x, y) => { x.ndot1 = y.ndot1; x })
      .map(ct => (ct, ct.lmi()))

    // ct_all.filter(_._1.a == "banana").map(_._1.prettyPrint()).print() // pretty-print for debugging purposes

    val ct_all_filtered = ct_all
      .filter(_._2 >= DSTaskConfig.param_min_sig)
      .groupBy("_1.a")
      .sortGroup("_2", Order.DESCENDING)
      .first(DSTaskConfig.param_topn_f)
      .map(_._1)

    ct_all_filtered
  }

}
