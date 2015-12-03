package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2, CT2Min}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object FilterSortDT {

  def CT2[T1 : TypeInformation, T2 : TypeInformation]() = new FilterSortDT__CT2[T1,T2]()

  def CT2Min[T1 : TypeInformation, T2 : TypeInformation]() = new FilterSortDT__CT2Min[T1,T2]()

}

class FilterSortDT__CT2[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(l => l.split("\t") match {
    case Array(a,b,n11) => CT2[T1,T2](a.asInstanceOf[T1], b.asInstanceOf[T2], n11.toFloat, n11.toFloat, n11.toFloat, n11.toFloat)
    case _ => CT2[T1,T2](null.asInstanceOf[T1],null.asInstanceOf[T2],0f)
  })

  // TODO: this can be optimized
  override def process(ds: DataSet[CT2[T1,T2]]): DataSet[CT2[T1,T2]] = {

    val dtf = ds
      .filter(_.n11 >= DSTaskConfig.min_sim) // number of co-occurrences
      .map(ct => {ct.n1dot = ct.n11; ct.ndot1 = 1; ct}) // misuse ndot1 as o1dot
      .groupBy("A")
      .reduce((l,r) => {l.B = null.asInstanceOf[T2]; l.n1dot += l.n1dot; l.ndot1 += l.ndot1; l})
      .filter(_.ndot1 >= DSTaskConfig.min_sim_distinct) // number of distinct co-occurrences

    val dtsort = ds
      .join(dtf)
      .where("A").equalTo("A")((x, y) => x)
      .groupBy("A")
      .sortGroup("n11", Order.DESCENDING)
      .first(DSTaskConfig.topn_s)

    dtsort
  }

}

class FilterSortDT__CT2Min[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2Min[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(l => l.split("\t") match {
    case Array(a,b,n11) => CT2Min[T1,T2](a.asInstanceOf[T1], b.asInstanceOf[T2], n11.toFloat)
    case _ => CT2Min[T1,T2](null.asInstanceOf[T1],null.asInstanceOf[T2],0f)
  })

  // TODO: this can be optimized
  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2Min[T1,T2]] = {

    val dtf = ds
      .filter(_.n11 >= DSTaskConfig.min_sim) // number of co-occurrences
      .map((_,1))
      .groupBy("_1.A")
      .sum("_2")
      .filter(_._2 >= DSTaskConfig.min_sim_distinct) // number of distinct co-occurrences
      .map(_._1)

    val dtsort = ds
      .join(dtf)
      .where("A").equalTo("A")((x, y) => x)
      .groupBy("A")
      .sortGroup("n11", Order.DESCENDING)
      .first(DSTaskConfig.topn_s)

    dtsort
  }

}
