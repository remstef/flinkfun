package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.{TextToCT2, CT2Min}
import org.apache.flink.api.scala._

/**
  * Created by sr on 12/1/15.
  */
class Extractor extends DtTask[String, CT2Min[String,String]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[String] = lineDS

  override def process(ds: DataSet[String]): DataSet[CT2Min[String,String]] = {
    val ct_raw:DataSet[CT2Min[String,String]] = ds
      .filter(_ != null)
      .filter(!_.trim().isEmpty())
      .flatMap(s => TextToCT2.ngram_patterns(s,5,3))
    ct_raw
  }



}
