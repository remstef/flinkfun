package de.tudarmstadt.lt.flinkdt.tasks

import java.io.File

import de.tudarmstadt.lt.flinkdt.{CT2, CT2Min}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object WhiteListFilter {

  def CT2[T2 : TypeInformation](whitelist:String, env:ExecutionEnvironment) = new WhiteListFilter__CT2[T2](whitelist, env)

  def CT2Min[T2 : TypeInformation](whitelist:String, env:ExecutionEnvironment) = new WhiteListFilter__CT2Min[T2](whitelist, env)

}


class WhiteListFilter__CT2[T2 : TypeInformation](whitelist:String, env:ExecutionEnvironment) extends DSTask[CT2[String, T2],CT2[String, T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[String,T2]] = lineDS.map(CT2.fromString[String,T2](_))

  override def process(ds: DataSet[CT2[String,T2]]): DataSet[CT2[String,T2]] = {

    if(whitelist == null)
      return ds

    val whiteterms = ( if (whitelist.contains('\n')) env.fromCollection(whitelist.split('\n')) else env.readTextFile(whitelist) )
      .filter(s => s.trim.length > 0)
      .map(Tuple1(_))
      .distinct(0)

    val white_cts_A = ds // get all contexts of whitelist terms
      .joinWithTiny(whiteterms) // assume that
      .where("a").equalTo(0)((x, y) =>  x )
      .distinct("b")

    val white_cts_B_from_white_cts_A = ds
      .join(white_cts_A)
      .where("b").equalTo("b")((x,y) => x) // get all terms of contexts of whitelist terms
      .distinct("a")

    val white_cts_A_from_white_cts_B = ds
      .join(white_cts_B_from_white_cts_A)
      .where("a").equalTo("a")((x,y) => x) // now get all the contexts of the new terms

    white_cts_A_from_white_cts_B

  }

}

class WhiteListFilter__CT2Min[T2 : TypeInformation](whitelist:String, env:ExecutionEnvironment) extends DSTask[CT2Min[String, T2],CT2Min[String, T2]] {

  val whitelistFilterWrapped = new WhiteListFilter__CT2[T2](whitelist, env)

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[String,T2]] = lineDS.map(CT2Min.fromString[String,T2](_))

  override def process(ds: DataSet[CT2Min[String,T2]]): DataSet[CT2Min[String,T2]] = {
    val ds_ct2:DataSet[CT2[String, T2]] = ds.map(_.toCT2())
    whitelistFilterWrapped.process(ds_ct2).map(_.toCT2Min())
  }

}

