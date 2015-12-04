package de.tudarmstadt.lt.flinkdt.tasks

import java.io.File

import de.tudarmstadt.lt.flinkdt.CT2Min
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object WhiteListFilter {

  def apply(whitelist:String, env:ExecutionEnvironment) = new WhiteListFilter[String](whitelist, env)

}

class WhiteListFilter[T2 : TypeInformation](whitelist:String, env:ExecutionEnvironment) extends DSTask[CT2Min[String,T2],CT2Min[String,T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[String,T2]] = lineDS.map(l => CT2Min.fromString[String,T2](l))

  override def process(ds: DataSet[CT2Min[String,T2]]): DataSet[CT2Min[String,T2]] = {
    if(whitelist == null)
      return ds
    val whiteterms = if(new File(whitelist).exists) env.readTextFile(whitelist) else env.fromCollection(whitelist.split('\n'))
      .filter(s => s.trim.length > 0)
      .map(Tuple1(_))
      .distinct(0)
    val white_cts_A = ds // get all contexts of whitelist terms
      .joinWithTiny(whiteterms) // assume that
      .where("a").equalTo(0)((x, y) =>  x )
      .distinct("b")
    val white_cts_B_from_white_cts_A = ds
      .joinWithTiny(white_cts_A)
      .where("b").equalTo("b")((x,y) => x) // get all terms of contexts of whitelist terms
    white_cts_B_from_white_cts_A
  }

}
