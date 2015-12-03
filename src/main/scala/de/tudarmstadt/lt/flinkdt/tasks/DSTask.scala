package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
@SerialVersionUID(42L)
trait DSTask[I, O] extends (DataSet[I] => DataSet[O]) with Serializable  {

  def fromLines(lineDS:DataSet[String]):DataSet[I]

  def toLines(ds:DataSet[O]):DataSet[String] = ds.map(_.toString())

  def process(ds:DataSet[I]):DataSet[O]

  def process(env:ExecutionEnvironment = null, inputtext:String = null):DataSet[O] = {
    process(fromLines(DSReader(inputtext,env).process(null)))
  }

  override def apply(ds: DataSet[I]): DataSet[O] = process(ds)

  def ~>[X] (g:DSTask[O,X]) = new DSTaskChain(this, g)

}







