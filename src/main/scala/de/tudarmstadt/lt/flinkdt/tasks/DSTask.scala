package de.tudarmstadt.lt.flinkdt.tasks

import java.io.File

import de.tudarmstadt.lt.utilities.TimeUtils
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

  def process(env:ExecutionEnvironment, inputtext:String, outputlocation:String = null):DataSet[O] = {
    val out =
      if(outputlocation != null){
        outputlocation
      }else{
        var t:DSTask[_,_] = this
        while(t.isInstanceOf[DSTaskChain[_,_,_]] || t.isInstanceOf[DSTaskWriterChain[_,_,_]])
          t = (t.asInstanceOf[DSTaskChain[I,_,O]]).g
        val name = s"${TimeUtils.getSimple17}_${t.getClass.getSimpleName}"
        new File(DSTaskConfig.out_basedir, name).getAbsolutePath
      }
    val ds_out = process(fromLines(DSReader(inputtext,env).process(null)))
    val writer:DSWriter[String] = new DSWriter[String](out)
    writer.process(toLines(ds_out))
    ds_out
  }

  override def apply(ds: DataSet[I]): DataSet[O] = process(ds)

  def ~>[X:TypeInformation](g:DSTask[O,X]) = new DSTaskChain[I,X,O](this, g)

  def ~|~>[X:TypeInformation](g:DSTask[O,X]) = new DSTaskWriterChain[I,X,O](this, g)

}







