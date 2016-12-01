package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.types.CT2red
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object Extractor {

  def apply(extractorfun:String => TraversableOnce[CT2red[String, String]], inputcolumn:Int = -1) = new Extractor(extractorfun, inputcolumn)

}

class Extractor(extractorfun:String => TraversableOnce[CT2red[String, String]], textcol:Int) extends DSTask[String, CT2red[String,String]] {

  override def process(ds: DataSet[String]): DataSet[CT2red[String,String]] = {

    val dsf = ds
      .filter(_ != null)
      .filter(!_.trim().isEmpty())

    val dsfc = if(textcol > -1) dsf.map(_.split("\t")(textcol)).filter(_.trim.length > 0) else dsf

    val ct_raw:DataSet[CT2red[String,String]] = dsfc
      .flatMap(s => extractorfun(s))

    ct_raw
  }

}
