package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object DSTask {

  def apply[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation](processfun:DataSet[I] => DataSet[O]) =
    new DSTask[I, O] {
      override def process(ds: DataSet[I]): DataSet[O] = processfun(ds)
    }

}

@SerialVersionUID(42L)
abstract class DSTask[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation] extends (DataSet[I] => DataSet[O]) with Serializable  {

  def process(ds:DataSet[I]):DataSet[O]

  def process(input:String, output:String = null, jobname:String = null, env: ExecutionEnvironment = null):DataSet[O] = {
    val ds_out = process(DSReader[I](input, null).process(null))
    if(output != null && !output.isEmpty)
      DSWriter[O](output, jobname).process(ds_out)
    ds_out
  }

  override def apply(ds: DataSet[I]): DataSet[O] = process(ds)

  def ~>[X : ClassTag : TypeInformation](g:DSTask[O,X]) = new DSTaskChain[I,X,O](this, g)

  def ~|~>[X : ClassTag : TypeInformation](g:DSTask[O,X]) = new DSTaskWriterChain[I,X,O](this, g, out = null, jobname = null)


}







