package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2, CT2Min}
import de.tudarmstadt.lt.scalautils.FormatUtils
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object FilterSortDT {

  def CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:CT2[_,_] => Float, order:Order = Order.DESCENDING, sort_B_desc_by_string:Boolean = false) = new FilterSortDT__CT2[T1,T2](valfun, order, sort_B_desc_by_string)

  def CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:CT2[_,_] => Float, order:Order = Order.DESCENDING, sort_B_desc_by_string:Boolean = false) = new FilterSortDT__CT2Min[T1,T2](valfun, order, sort_B_desc_by_string)

  def CT2Min_CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:CT2[_,_] => Float, order:Order = Order.DESCENDING, sort_B_desc_by_string:Boolean = false) = new FilterSortDT__CT2Min_CT2[T1,T2](valfun, order, sort_B_desc_by_string)

}

class FilterSortDT__CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:(CT2[_,_] => Float), order:Order, sort_B_desc_by_string:Boolean) extends DSTask[CT2[T1,T2],(CT2[T1,T2], Float)] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  override def toLines(ds: DataSet[(CT2[T1, T2], Float)]): DataSet[String] = ds.map(_ match {
    case (ct, v) => {
      val sv = FormatUtils.format(v)
      val sa = ct.toStringArray()
      s"${sa.slice(0,2).mkString("\t")}\t${sv}\t${sa.slice(2,sa.length).mkString("\t")}"
    }
  })

  // TODO: this can surely be optimized
  override def process(ds: DataSet[CT2[T1,T2]]): DataSet[(CT2[T1,T2], Float)] = {

    val ds_f = ds.filter(valfun(_) >= DSTaskConfig.param_min_sim)

    val dt_count = ds_f
      .map(ct => (ct.a, 1)) // o1dot
      .groupBy(0)
      .sum(1)

    val dt_val_grouped_a = ds_f
      .join(dt_count)
      .where("a").equalTo(0)((l, r) => (l, r._2))
      .filter(_._2 >= DSTaskConfig.param_min_sim_ndistinct) // number of distinct co-occurrences (o1dot)
      .map(t => (t._1, valfun(t._1))) // apply valfun
      .groupBy("_1.a")

    val dt_sort_val =
      if(sort_B_desc_by_string) {
        dt_val_grouped_a
          .reduceGroup((iter, out: Collector[(CT2[T1, T2], Float)]) => {
            val l = iter.toSeq
            l.sortBy(t => (if(order == Order.DESCENDING) -t._2 else t._2, t._1.b.toString)) // sort by value and ascending by B.toString
              .take(DSTaskConfig.param_topn_s)
              .foreach(out.collect)
          })
      }else {
        dt_val_grouped_a
          .sortGroup(1, order)
          .first(DSTaskConfig.param_topn_s)
      }

    dt_sort_val
  }

}

class FilterSortDT__CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:(CT2[_,_] => Float), order:Order, sort_B_desc_by_string:Boolean) extends DSTask[CT2Min[T1,T2],(CT2Min[T1,T2], Float)] {

  val wrapped_CT2Min_CT2 = new FilterSortDT__CT2Min_CT2[T1,T2](valfun, order, sort_B_desc_by_string)

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = wrapped_CT2Min_CT2.fromLines(lineDS)

  override def toLines(ds: DataSet[(CT2Min[T1, T2], Float)]): DataSet[String] = ds.map(_ match {
    case (ct, v) => {
      val sv = FormatUtils.format(v)
      val sa = ct.toStringArray()
      s"${sa.slice(0,2).mkString("\t")}\t${sv}\t${sa.slice(2,sa.length).mkString("\t")}"
    }
  })

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[(CT2Min[T1,T2], Float)] = wrapped_CT2Min_CT2.process(ds).map(t => (t._1.toCT2Min, t._2))

}

class FilterSortDT__CT2Min_CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:(CT2[_,_] => Float), order:Order, sort_B_desc_by_string:Boolean) extends DSTask[CT2Min[T1,T2],(CT2[T1,T2], Float)] {

  val wrapped_CT2 = new FilterSortDT__CT2[T1,T2](valfun, order, sort_B_desc_by_string)

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString[T1,T2](_))

  override def toLines(ds: DataSet[(CT2[T1, T2], Float)]): DataSet[String] = wrapped_CT2.toLines(ds)

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[(CT2[T1,T2], Float)] = wrapped_CT2.process(ds.map(_.toCT2()))

}



