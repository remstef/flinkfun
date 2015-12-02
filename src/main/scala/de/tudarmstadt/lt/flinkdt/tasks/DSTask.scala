package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
@SerialVersionUID(42L)
trait DSTask[I, O] extends (DataSet[I] => DataSet[O]) with Serializable { // later: [O :> CT]

  def fromLines(lineDS:DataSet[String]):DataSet[I]

  def toLines(ds:DataSet[O]):DataSet[String] = ds.map(_.toString())

  def process(ds:DataSet[I]):DataSet[O]

  def process():DataSet[O] = throw new IllegalStateException(s"${getClass.toString}: Method cannot be executed on this task.")

  override def apply(ds: DataSet[I]): DataSet[O] = process(ds)

  def ~>[X] (g:DSTask[O,X]) = new DSTaskChain(this, g)

}







