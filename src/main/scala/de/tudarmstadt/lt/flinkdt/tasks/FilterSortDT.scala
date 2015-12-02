package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.CT2Min
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object FilterSortDT {

  def String() = new FilterSortDT[String,String,CT2Min[String,String]]()

  def Int() = new FilterSortDT[Int,Int,CT2Min[Int,Int]]()

}

class FilterSortDT[T1, T2, K <: CT2Min[T1,T2] : ClassTag : TypeInformation] extends DSTask[K,K] {

  override def fromLines(lineDS: DataSet[String]): DataSet[K] = lineDS.map(l => l.split("\t") match {
    case Array(a,b,n11) => CT2Min[T1,T2](a.asInstanceOf[T1], b.asInstanceOf[T2], n11.toFloat).asInstanceOf[K]
    case _ => CT2Min[T1,T2](null.asInstanceOf[T1],null.asInstanceOf[T2],0f).asInstanceOf[K]
  })

  // TODO: this can be optimized
  override def process(ds: DataSet[K]): DataSet[K] = {

    val dtf = ds
      .filter(_.n11 > 1) // number of co-occurrences
      .map((_,1))
      .groupBy("_1.A")
      .sum("_2")
      .filter(_._2 > 1) // number of distinct co-occurrences
      .map(_._1)

    val dtsort = ds
      .join(dtf)
      .where("A").equalTo("A")((x, y) => x)
      .groupBy("A")
      .sortGroup("n11", Order.DESCENDING)
      .first(200)

    dtsort
  }

}
