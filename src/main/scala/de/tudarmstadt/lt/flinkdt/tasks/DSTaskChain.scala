package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
class DSTaskChain[I : TypeInformation, O : TypeInformation, X : TypeInformation](val f:DSTask[I,X], val g:DSTask[X,O]) extends DSTask[I,O] {

  override def fromLines(lineDS: DataSet[String]): DataSet[I] = f.fromLines(lineDS)

  override def process(ds: DataSet[I]): DataSet[O] = g(f(ds))

}
