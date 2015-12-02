package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.CT2Min
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object N11Sum {

  def String() = new N11Sum[String,String,CT2Min[String,String]]()

  def Int() = new N11Sum[Int,Int,CT2Min[Int,Int]]()

}

class N11Sum[T1, T2, K <: CT2Min[T1,T2] : ClassTag : TypeInformation] extends DSTask[K,K] {

  override def fromLines(lineDS: DataSet[String]): DataSet[K] = lineDS.map(l => l.split("\t") match {
    case Array(a,b,n11) => CT2Min[T1,T2](a.asInstanceOf[T1], b.asInstanceOf[T2], n11.toFloat).asInstanceOf[K]
    case _ => CT2Min[T1,T2](null.asInstanceOf[T1],null.asInstanceOf[T2],0f).asInstanceOf[K]
  })

  override def process(ds: DataSet[K]): DataSet[K] = {
    ds.groupBy("A","B")
      .sum("n11")
      .filter(_.n11 > 1)
  }

}
