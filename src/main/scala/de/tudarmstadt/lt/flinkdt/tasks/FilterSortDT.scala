package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types.CT2
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object FilterSortDT {

  def apply[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:C => Float, order:Order = Order.DESCENDING, sort_B_desc_by_string:Boolean = false) = new FilterSortDT[C, T1, T2](valfun, order, sort_B_desc_by_string)

}


class FilterSortDT[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](valfun:(C => Float), order:Order, sort_B_desc_by_string:Boolean) extends DSTask[C, C] {

  override def fromLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C,T1,T2](_))

  // TODO: this can surely be optimized
  override def process(ds: DataSet[C]): DataSet[C] = {

    val ds_f = ds.filter(valfun(_) >= DSTaskConfig.param_min_sim)

    val dt_count = ds_f
      .map(ct => (ct.a.asInstanceOf[T1], 1)) // o1dot
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
          .reduceGroup((iter, out: Collector[(C, Float)]) => {
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

    dt_sort_val.map(_._1)
  }

}
