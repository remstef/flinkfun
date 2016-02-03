package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object DSTask {

  def apply[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation](stringfun:String => I, processfun:DataSet[I] => DataSet[O]) =
    new DSTask[I, O] {
      override def fromLines(lineDS: DataSet[String]): DataSet[I] = lineDS.map(stringfun(_))
      override def process(ds: DataSet[I]): DataSet[O] = processfun(ds)
    }

}

@SerialVersionUID(42L)
abstract class DSTask[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation] extends (DataSet[I] => DataSet[O]) with Serializable  {

  def fromLines(lineDS:DataSet[String]):DataSet[I]

  def toLines(ds:DataSet[O]):DataSet[String] = ds.map(_.toString())

  def process(ds:DataSet[I]):DataSet[O]

  def process(ds:DataSet[I], output:String):DataSet[O] = {
    if(output != null && !output.isEmpty) {
      val ds_out = process(ds)
      val writer: DSWriter[String] = new DSWriter[String](output)
      writer.process(toLines(ds_out))
      ds_out
    }
    else
      process(ds)
  }

  def process(env:ExecutionEnvironment, input:String, output:String = null):DataSet[O] = {
    val ds_out = process(fromLines(DSReader(input, env).process(null)))
    if(output != null && !output.isEmpty) {
      val writer: DSWriter[String] = new DSWriter[String](output)
      writer.process(toLines(ds_out))
    }
    ds_out
  }

  override def apply(ds: DataSet[I]): DataSet[O] = process(ds)

  def ~>[X : ClassTag : TypeInformation](g:DSTask[O,X]) = new DSTaskChain[I,X,O](this, g)

  def ~|~>[X : ClassTag : TypeInformation](g:DSTask[O,X]) = new DSTaskWriterChain[I,X,O](this, g)

}







