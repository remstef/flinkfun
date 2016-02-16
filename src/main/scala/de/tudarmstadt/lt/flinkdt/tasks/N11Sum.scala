package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object N11Sum {

  def apply[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new N11Sum[C, T1, T2]()

}

class N11Sum[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[C, C] {

  override def fromInputLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C,T1,T2](_))

  override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C,T1,T2](_))

  override def process(ds: DataSet[C]): DataSet[C] = {
    ds.groupBy("a","b")
//      .reduce((l,r) => {l.n11 += r.n11; l}) // .sum("n11")
      .sum("n11")
  }

}
