package de.tudarmstadt.lt.flinkdt.tasks

import java.io.File

import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object DSReader {

  def apply(in:String, env:ExecutionEnvironment) = new DSReader(in,env)

}

class DSReader(in: String, env: ExecutionEnvironment) extends DSTask[String, String]{

  override def fromLines(lineDS: DataSet[String]): DataSet[String] = lineDS

  override def process(ds: DataSet[String]): DataSet[String] = ds

  override def process(): DataSet[String] = {
    val ds = if(new File(in).exists) env.readTextFile(in) else env.fromCollection(in.split('\n'))
    ds
  }

}
