package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object DSTask {

  def apply[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation](stringfun:String => I, processfun:DataSet[I] => DataSet[O], cpfun:String => O) =
    new DSTask[I, O] {
      override def fromInputLines(lineDS: DataSet[String]): DataSet[I] = lineDS.map(stringfun(_))
      override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[O] = lineDS.map(cpfun(_))
      override def process(ds: DataSet[I]): DataSet[O] = processfun(ds)
    }

}

@SerialVersionUID(42L)
abstract class DSTask[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation] extends (DataSet[I] => DataSet[O]) with Serializable  {

  def fromInputLines(lineDS:DataSet[String]):DataSet[I]

  def fromCheckpointLines(lineDS:DataSet[String]):DataSet[O]

  def toLines(ds:DataSet[O]):DataSet[String] = ds.map(_.toString())

  def process(ds:DataSet[I]):DataSet[O]

  def process(ds:DataSet[I], output:String, jobname:String):DataSet[O] = {
    if(output != null && !output.isEmpty) {
      val ds_out = process(ds)
      val writer: DSWriter[String] = new DSWriter[String](output, jobname)
      writer.process(toLines(ds_out))
      ds_out
    }
    else
      process(ds)
  }

  def process(input:String, output:String = null, jobname:String = null):DataSet[O] = {
    val ds_out = process(fromInputLines(DSReader(input).process(null)))
    if(output != null && !output.isEmpty) {
      val writer: DSWriter[String] = new DSWriter[String](output, jobname)
      writer.process(toLines(ds_out))
    }
    ds_out
  }

  override def apply(ds: DataSet[I]): DataSet[O] = process(ds)

  def ~>[X : ClassTag : TypeInformation](g:DSTask[O,X]) = new DSTaskChain[I,X,O](this, g)

  def ~|~>[X : ClassTag : TypeInformation](g:DSTask[O,X]) = new DSTaskWriterChain[I,X,O](this, g, out = null, jobname = null)


}







