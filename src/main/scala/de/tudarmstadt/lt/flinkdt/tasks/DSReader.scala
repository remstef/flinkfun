package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.scala._
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import scala.reflect.ClassTag
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.io.FileInputFormat
import scala.reflect._

/**
  * Created by Steffen Remus
  */
object DSReader {

  def apply(in:String, env: ExecutionEnvironment = null) = new DSReader[String](in, env)
  
  def apply[T : ClassTag : TypeInformation](in:String, env: ExecutionEnvironment) = new DSReader[T](in, env)

}

class DSReader[T : ClassTag : TypeInformation](in: String, env: ExecutionEnvironment) extends DSTask[String, T]{

  override def process(ds: DataSet[String]): DataSet[T] = {
    if(ds != null)
      throw new IllegalArgumentException(s"${getClass.getSimpleName} does not expect a dataset. Reconsider your pipeline.")
    process()
  }

  def process(): DataSet[T] = {
    val env_ =
      if(env == null)
        ExecutionEnvironment.getExecutionEnvironment
      else
        env
    val ds = if(in.contains('\n')){
      env_.fromCollection(in.split('\n')).map(_.asInstanceOf[T])
    }
    else
      env.readCsvFile[T](in, fieldDelimiter="\t")

    if(classTag[T] == classTag[String])
      return ds.filter(!_.asInstanceOf[String].isEmpty())
    
    ds
    
  }

}
