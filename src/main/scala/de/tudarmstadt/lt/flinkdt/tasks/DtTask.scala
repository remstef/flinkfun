package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
trait DtTask[I,O] { // later: [O :> CT]

  def fromLines(lineDS:DataSet[String]):DataSet[I]

  def toLines(ds:DataSet[O]):DataSet[String] = ds.map(_.toString())

  def process(ds:DataSet[I]):DataSet[O]



}
