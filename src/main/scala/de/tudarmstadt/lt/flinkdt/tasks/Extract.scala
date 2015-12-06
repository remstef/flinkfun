package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{TextToCT2, CT2Min}
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object Extractor {

  def apply(extractorfun:String => TraversableOnce[CT2Min[String, String]] = s => Seq.empty[CT2Min[String,String]]) = new Extractor(extractorfun)

}

class Extractor(extractorfun:String => TraversableOnce[CT2Min[String, String]]) extends DSTask[String, CT2Min[String,String]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[String] = lineDS

  override def process(ds: DataSet[String]): DataSet[CT2Min[String,String]] = {
    val ct_raw:DataSet[CT2Min[String,String]] = ds
      .filter(_ != null)
      .filter(!_.trim().isEmpty())
      .flatMap(s => extractorfun(s))
    ct_raw
  }

}
