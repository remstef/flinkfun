package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2Min, CT2}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object ComputeCT2 {

  def apply[T1 : TypeInformation, T2 : TypeInformation]() = new ComputeCT2[T1,T2]()

}

class ComputeCT2[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString(_))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2[T1,T2]] = {

    val cts:DataSet[CT2[T1,T2]] = ds.map(ct => CT2[T1,T2](ct.a, ct.b, n11 = ct.n11, n1dot = ct.n11, ndot1 = ct.n11, n = ct.n11))

    val ct_accumulated_A = cts
      .groupBy("a")
      .reduce((l,r) => {l.n1dot = l.n1dot+r.n1dot; l})
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)

//    writeIfExists("accA", ct_accumulated_A)

    val ct_accumulated_B = cts
      .map(ct => {ct.ndot1 = ct.n11; ct.n = 1f; ct}) // misuse n as odot1 i.e. the number of distinct occurrences of feature B (parameter wc=wordcount or wpfmax=wordsperfeature in traditional jobimtext)
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 = l.ndot1 + r.ndot1; l.n = l.n + r.n; l})
      .filter(ct => ct.n <= DSTaskConfig.param_max_odot1 && ct.n >= DSTaskConfig.param_min_odot1)

//    writeIfExists("accB", ct_accumulated_B)

    val n = cts.map(ct => ct.n11).reduce((l,r) => l+r)
    val ct_accumulated_n = cts.crossWithTiny(n)((ct,n) => {ct.n = n; ct})

    val ct_all = ct_accumulated_n
      .join(ct_accumulated_A)
      .where("a")
      .equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
      .join(ct_accumulated_B)
      .where("b")
      .equalTo("b")((x, y) => { x.ndot1 = y.ndot1; x })
      .map(ct => (ct, ct.lmi()))

    ct_all.map(_._1.prettyPrint()).print()
//    writeIfExists("accall", ct_all)

    val ct_all_filtered = ct_all
      .filter(_._2 >= DSTaskConfig.param_min_sig)
      .groupBy("_1.a")
      .sortGroup("_2", Order.DESCENDING)
      .first(DSTaskConfig.param_topn_f)
      .map(_._1)

    ct_all_filtered
  }



}
