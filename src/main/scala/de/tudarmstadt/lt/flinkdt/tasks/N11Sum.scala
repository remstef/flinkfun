package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.types._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object N11Sum {

  def apply[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] = new N11Sum[C, T1, T2]()

  def toCT2withN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new N11Sum__withN[T1,T2]()

}

class N11Sum[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[C, C] {

  override def fromLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C,T1,T2](_))

  override def process(ds: DataSet[C]): DataSet[C] = {
    ds.groupBy("a","b")
//      .reduce((l,r) => {l.n11 += r.n11; l}) // .sum("n11")
      .sum("n11")
  }

}

class N11Sum__withN[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2red[T1,T2],CT2def[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2red[T1,T2]] = lineDS.map(CtFromString[CT2red[T1,T2], T1, T2](_))

  override def process(ds: DataSet[CT2red[T1,T2]]): DataSet[CT2def[T1,T2]] = {
    val ct_sum_n11 = ds.groupBy("a","b")
      .reduce((l,r) => {l.n11 += r.n11; l}) // .sum("n11")


    val n = ct_sum_n11.map(ct => ct.n11).reduce((l,r) => l+r)
    val ct_sum = ct_sum_n11
      .crossWithTiny(n)((ct,n) => ct.asCT2Full(n = n, n1dot = ct.n11, ndot1 = ct.n11))

    ct_sum

  }

}