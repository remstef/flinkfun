package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2, CT2Min}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object FilterSortDT {

  def CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new FilterSortDT__CT2[T1,T2]()

  def CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new FilterSortDT__CT2Min[T1,T2]()

  def CT2Min_CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new FilterSortDT__CT2Min_CT2[T1,T2]()

}

class FilterSortDT__CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](sort_B_by_string:Boolean = false) extends DSTask[CT2[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  // TODO: this can surely be optimized
  override def process(ds: DataSet[CT2[T1,T2]]): DataSet[CT2[T1,T2]] = {

    val ds_f = ds.filter(_.n11 >= DSTaskConfig.param_min_sim) // number of co-occurrences

    val dt_count = ds_f
      .map(ct => {ct.n1dot = ct.n11; ct.ndot1 = 1; ct}) // misuse ndot1 as o1dot
      .groupBy("a")
      .reduce((l,r) => {l.b = null.asInstanceOf[T2]; l.n1dot += r.n1dot; l.ndot1 += r.ndot1; l})

    val dt_grouped_a = ds_f
      .join(dt_count)
      .where("a").equalTo("a")((l, r) => {l.n1dot = r.n1dot; l.ndot1 = r.ndot1; l})
      .filter(_.ndot1 >= DSTaskConfig.param_min_sim_ndistinct) // number of distinct co-occurrences
      .groupBy("a")

    val dt_sort =
      if(sort_B_by_string) {
        dt_grouped_a
          .reduceGroup((iter, out: Collector[CT2[T1, T2]]) => {
            val l = iter.toSeq
            l.sortBy(ct => (-ct.n11, ct.b.toString)) // sort descending by value and ascending by B
              .take(DSTaskConfig.param_topn_s)
              .foreach(out.collect)
          })
      }else {
        dt_grouped_a
          .sortGroup("n11", Order.DESCENDING)
          .first(DSTaskConfig.param_topn_s)
      }

    dt_sort
  }

}

class FilterSortDT__CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2Min[T1,T2]] {

  val wrapped_CT2Min_CT2    = new FilterSortDT__CT2Min_CT2[T1,T2]()

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = wrapped_CT2Min_CT2.fromLines(lineDS)

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2Min[T1,T2]] = wrapped_CT2Min_CT2.process(ds).map(_.toCT2Min)

}

class FilterSortDT__CT2Min_CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2[T1,T2]] {

  val wrapped_CT2    = new FilterSortDT__CT2[T1,T2]()

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString[T1,T2](_))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2[T1,T2]] = wrapped_CT2.process(ds.map(_.toCT2()))

}



