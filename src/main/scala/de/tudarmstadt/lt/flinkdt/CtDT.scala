package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

import scala.math._
import scala.util.Try

object CtDT extends App {

  val config:Config =
    if(args.length > 0)
      ConfigFactory.parseFile(new File(args(0))).withFallback(ConfigFactory.load()).resolve() // load conf with fallback to default application.conf
    else
      ConfigFactory.load() // load default application.conf

  val config_dt = config.getConfig("DT")
  val outputconfig = config_dt.getConfig("output.ct")
  val outputbasedir = new File(if(config_dt.hasPath("output.basedir")) config_dt.getString("output.basedir") else "./")
  if(!outputbasedir.exists())
    outputbasedir.mkdirs()
  val pipe = outputconfig.getStringList("pipeline").toArray

  def writeIfExists[T1 <: Any, T2 <: Any](conf_path:String, ds:DataSet[CT2[T1,T2]], stringfun:((CT2[T1,T2]) => String) = ((ct2:CT2[T1,T2]) => ct2.toString)): Unit = {
    if(outputconfig.hasPath(conf_path)){
      val o = ds.map(stringfun).map(Tuple1(_))
      if(outputconfig.getString(conf_path) equals "stdout") {
        o.print()
      }
      else{
        o.writeAsCsv(new File(outputbasedir, outputconfig.getString(conf_path)).getAbsolutePath, "\n", "\t", writeMode = FileSystem.WriteMode.OVERWRITE)
        if(pipe(pipe.size-1) == conf_path) {
          env.execute("CtDT")
          return
        }
      }
    }
  }

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = config_dt.getString("input.text")

  val text:DataSet[String] = if(new File(in).exists) env.readTextFile(in) else env.fromCollection(in.split('\n'))

  val ct_raw:DataSet[CT2[String,String]] = text
    .filter(_ != null)
    .filter(!_.trim().isEmpty())
    .map(TextToCT2.ngram_patterns(_,5,3))
    .flatMap(s => Util.collapse(s))

  writeIfExists("raw", ct_raw)

  val ct_raw_white = if(config_dt.hasPath("input.whitelist") && new File(config_dt.getString("input.whitelist")).exists) {
    val whitelist = env.readTextFile(config_dt.getString("input.whitelist")).map(Tuple1(_)).distinct(0)
    val white_cts_A = ct_raw // get all contexts of whitelist terms
      .joinWithTiny(whitelist)
      .where("A").equalTo(0)((x, y) =>  x )
      .distinct(0)
    val white_cts_B_from_white_cts_A = ct_raw
      .joinWithTiny(white_cts_A)
      .where("B").equalTo("B")((x,y) => x) // get all terms of contexts of whitelist terms
    writeIfExists("whiteraw", white_cts_B_from_white_cts_A)
    white_cts_B_from_white_cts_A
  }else{
    ct_raw
  }

  val ct_accumulated = ct_raw_white.groupBy("A","B")
    .sum("n11")
    .filter(_.n11 > 1)

  writeIfExists("accAB", ct_accumulated)

  val ct_accumulated_A = ct_raw_white.map(ct => {ct.n1dot=ct.n11; ct})
    .groupBy("A")
    .reduce((x,y) => x.copy(B="@", n1dot = x.n1dot+y.n1dot))
    .filter(_.n1dot > 1)

  writeIfExists("accA", ct_accumulated_A)

  val ct_accumulated_B = ct_raw_white.map(ct => {ct.ndot1 = ct.n11; ct})
    .groupBy("B")
    .reduce((x,y) => x.copy(A="@", ndot1 = x.ndot1 + y.ndot1))
    .filter(ct => ct.ndot1 > 1 && ct.ndot1 <= 1000)

  writeIfExists("accB", ct_accumulated_B)

  val n = Try(ct_accumulated.map(ct => ct.n11).reduce(_+_).collect()(0)).getOrElse(0f)
  println(n)

  val ct_all = ct_accumulated
    .join(ct_accumulated_A)
    .where("A")
    .equalTo("A")((x, y) => { x.n1dot = y.n1dot; x })
    .join(ct_accumulated_B)
    .where("B")
    .equalTo("B")((x, y) => { x.ndot1 = y.ndot1; x })
    .map(ct => {ct.n = n; ct.n11 = ct.lmi(); ct})

  writeIfExists("accall", ct_all)

  val ct_all_filtered = ct_all
    .groupBy("A")
    .sortGroup("n11", Order.DESCENDING)
    .first(1000)

  val joined = ct_all_filtered
    .join(ct_all_filtered)
    .where("B")
    .equalTo("B")

  val dt = joined.map(cts => CT2(cts._1.A, cts._2.A, n11=1f))
    .groupBy("A", "B")
    .sum("n11")
    .filter(_.n11 > 1)

  val dtf = dt
    .groupBy("A")
    .sum("n1dot")
    .filter(_.n1dot > 2)

  val dtsort = dt
    .join(dtf)
    .where("A").equalTo("A")((x, y) => { x.n1dot = y.n1dot; x })
    .groupBy("A")
    .sortGroup("n11", Order.DESCENDING)
    .first(100)

  writeIfExists("dt", dtsort)


}
