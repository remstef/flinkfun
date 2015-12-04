package de.tudarmstadt.lt.flinkdt.tasks


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem


/**
  * Created by Steffen Remus
  */
object DSWriter {

  def apply[T : TypeInformation](out:String) = new DSWriter[T](out)

}

class DSWriter[T : TypeInformation](out:String) extends DSTask[T,T] {

  override def fromLines(lineDS: DataSet[String]): DataSet[T] = ???

  override def process(ds: DataSet[T]): DataSet[T] = {
    if (out == null)
      return ds
    val o = ds.map(_.toString).map(Tuple1(_))
    if(out == "stdout")
      o.print()
    else
      o.writeAsCsv(out, "\n", "\t", writeMode = FileSystem.WriteMode.OVERWRITE)
    ds
  }

}
