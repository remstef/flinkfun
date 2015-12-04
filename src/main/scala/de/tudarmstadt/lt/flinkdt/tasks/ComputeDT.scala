package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{CT2Min, CT2}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus on 12/3/15.
  */
object ComputeDT {

  def fromCT2[T1 : TypeInformation, T2 : TypeInformation]() = new ComputeDT__CT2[T1,T2]()

}

class ComputeDT__CT2[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2[T1,T2], CT2Min[T1,T1]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(l => CT2.fromString(l))

  override def process(ds: DataSet[CT2[T1, T2]]): DataSet[CT2Min[T1, T1]] = {
    val joined:DataSet[CT2Min[T1,T1]] = ds
      .join(ds)
      .where("b")
      .equalTo("b")((l,r) => CT2Min[T1,T1](l.a, r.a, n11=1f))

    val dt = joined
      .groupBy("a", "b")
      .sum("n11")

    dt
  }

}
