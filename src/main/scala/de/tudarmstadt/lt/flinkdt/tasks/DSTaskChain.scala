package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
class DSTaskChain[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation, X : ClassTag : TypeInformation](val f:DSTask[I,X], val g:DSTask[X,O]) extends DSTask[I,O] {

  override def fromInputLines(lineDS: DataSet[String]): DataSet[I] = f.fromInputLines(lineDS)

  override def process(ds: DataSet[I]): DataSet[O] = g(f(ds))

  override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[O] = g.fromCheckpointLines(lineDS)
}
