package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
@SerialVersionUID(42L)
abstract class DSTask[I:TypeInformation, O:TypeInformation] extends (DataSet[I] => DataSet[O]) with Serializable  {

  def fromLines(lineDS:DataSet[String]):DataSet[I]

  def toLines(ds:DataSet[O]):DataSet[String] = ds.map(_.toString())

  def process(ds:DataSet[I]):DataSet[O]

  def process(env:ExecutionEnvironment, input:String, outputlocation:String = null):DataSet[O] = {
    val ds_out = process(fromLines(DSReader(input,env).process(null)))
    if(outputlocation != null && !outputlocation.isEmpty) {
      val writer: DSWriter[String] = new DSWriter[String](outputlocation)
      writer.process(toLines(ds_out))
    }
    ds_out
  }

  override def apply(ds: DataSet[I]): DataSet[O] = process(ds)

  def ~>[X:TypeInformation](g:DSTask[O,X]) = new DSTaskChain[I,X,O](this, g)

  def ~|~>[X:TypeInformation](g:DSTask[O,X]) = new DSTaskWriterChain[I,X,O](this, g)

}







