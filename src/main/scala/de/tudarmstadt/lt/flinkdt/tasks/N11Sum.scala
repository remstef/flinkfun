package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2, CT2Min}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object N11Sum {

  def toCT2Min[T1 : TypeInformation, T2 : TypeInformation]() = new N11Sum[T1,T2]()

  def toCT2withN[T1 : TypeInformation, T2 : TypeInformation]() = new N11Sum__withN[T1,T2]()

}

class N11Sum[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2Min[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString(_))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2Min[T1,T2]] = {
    ds.groupBy("a","b")
      .reduce((l,r) => {l.n11 += r.n11; l}) // .sum("n11")
      .filter(_.n11 >= DSTaskConfig.param_min_n11)
  }

}


class N11Sum__withN[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString(_))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2[T1,T2]] = {
    val ct_sum_n11 = ds.groupBy("a","b")
      .reduce((l,r) => {l.n11 += r.n11; l}) // .sum("n11")
      .filter(_.n11 >= DSTaskConfig.param_min_n11)

    val n = ct_sum_n11.map(ct => ct.n11).reduce((l,r) => l+r)
    val ct_sum = ct_sum_n11
      .crossWithTiny(n)((ct,n) => ct.toCT2(n = n))

    ct_sum
  }

}