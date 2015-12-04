package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.CT2Min
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object N11Sum {

  def apply[T1 : TypeInformation, T2 : TypeInformation]() = new N11Sum[T1,T2]()

}

class N11Sum[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2Min[T1,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(CT2Min.fromString(_))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2Min[T1,T2]] = {
    ds.groupBy("a","b")
      .reduce((l,r) => {l.n11 += r.n11; l}) // .sum("n11")
      .filter(_.n11 >= DSTaskConfig.param_min_n11)
  }

}
