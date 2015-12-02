package de.tudarmstadt.lt.flinkdt.tasks


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

import scala.reflect.ClassTag


/**
  * Created by Steffen Remus
  */
object DSWriter {

  def apply[T : ClassTag : TypeInformation](out:String) = new DSWriter[T](out, _.toString)

}

class DSWriter[T : ClassTag : TypeInformation](val out:String, val stringfun:(T => String)) extends DSTask[T,T] {

  override def fromLines(lineDS: DataSet[String]): DataSet[T] = ???

  override def process(ds: DataSet[T]): DataSet[T] = {
    val o = ds.map(stringfun(_)).map(Tuple1(_))
    if(out == "stdout")
      o.print()
    else
      o.writeAsCsv(out, "\n", "\t", writeMode = FileSystem.WriteMode.OVERWRITE)
    ds
  }

}
