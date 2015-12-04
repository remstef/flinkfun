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

  def CT2Min_CT2[T1 : TypeInformation, T2 : TypeInformation]() = new FilterSortDT__CT2Min_CT2[T1,T2]()

}

class FilterSortDT__CT2[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(CT2.fromString(_))

  // TODO: this can surely be optimized
  override def process(ds: DataSet[CT2[T1,T2]]): DataSet[CT2[T1,T2]] = {

    val dt_count = ds
      .map(ct => {ct.n1dot = ct.n11; ct.ndot1 = 1; ct}) // misuse ndot1 as o1dot
      .groupBy("A")
      .reduce((l,r) => {l.b = null.asInstanceOf[T2]; l.n1dot += l.n11; l.ndot1 += r.ndot1; l})

    val dtsort = ds
      .join(dt_count)
      .where("A").equalTo("A")((l, r) => {l.n1dot = r.n1dot; l.ndot1 = r.ndot1; l})
      .filter(_.n11 >= DSTaskConfig.param_min_sim) // number of co-occurrences
      .filter(_.ndot1 >= DSTaskConfig.param_min_sim_distinct) // number of distinct co-occurrences
      .groupBy("A")
      .sortGroup("n11", Order.DESCENDING)
      .first(DSTaskConfig.param_topn_s)

    dtsort
  }

}

class FilterSortDT__CT2Min[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2Min[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString(_))

  // TODO: this can surely be optimized
  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2Min[T1,T2]] = {

    val dtf = ds
      .map((_,1))
      .groupBy("_1.A")
      .sum("_2")

    val dtsort = ds
      .join(dtf)
      .where("A").equalTo("_1.A")((x, y) => (x, y._2))
      .filter(_._1.n11 >= DSTaskConfig.param_min_sim) // number of co-occurrences
      .filter(_._2 >= DSTaskConfig.param_min_sim_distinct) // number of distinct co-occurrences
      .groupBy("_1.A")
      .sortGroup("_2", Order.DESCENDING)
      .first(DSTaskConfig.param_topn_s)
      .map(_._1)

    dtsort
  }

}


class FilterSortDT__CT2Min_CT2[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2[T1,T2]] {

  val wrapped_CT2    = new FilterSortDT__CT2[T1,T2]()
  val wrapped_CT2Min = new FilterSortDT__CT2Min[T1,T2]()

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = wrapped_CT2Min.fromLines(lineDS)


  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2[T1,T2]] = {

    val ds_ct2 = ds.map(ctm => CT2[T1,T2](ctm.a, ctm.b, ctm.n11, ctm.n11, ctm.n11, ctm.n11))

    return wrapped_CT2.process(ds_ct2)

  }

}



