package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.scala.DataSet

/**
  * Created by Steffen Remus
  */
class DSTaskChain[I,O,X](f:DSTask[I,X], g:DSTask[X,O]) extends DSTask[I,O] {

  override def fromLines(lineDS: DataSet[String]): DataSet[I] = f.fromLines(lineDS)

  override def process(ds: DataSet[I]): DataSet[O] = g(f(ds))

}
