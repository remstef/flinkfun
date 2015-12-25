package de.tudarmstadt.lt.flinkdt.tasks

import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object DSReader {

  def apply(in:String, env:ExecutionEnvironment) = new DSReader(in,env)

}

class DSReader(in: String, env: ExecutionEnvironment) extends DSTask[String, String]{

  override def fromLines(lineDS: DataSet[String]): DataSet[String] = lineDS

  override def process(ds: DataSet[String]): DataSet[String] = {
    if(ds != null)
      throw new IllegalArgumentException(s"${getClass.getSimpleName} does not expect a dataset. Reconsider your pipeline.")
    process()
  }

  def process(): DataSet[String] = {
    val ds = if(in.contains('\n'))
      env.fromCollection(in.split('\n'))
    else
      env.readTextFile(in)
    ds.filter(_.trim.length > 0)
  }

}
