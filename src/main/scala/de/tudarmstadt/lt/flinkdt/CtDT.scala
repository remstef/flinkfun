package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

import scala.math._

object CtDT {
  def main(args: Array[String]) {

    var conf:Config = null
    if(args.length > 0)
      conf = ConfigFactory.parseFile(new File(args(0))).resolve() // load conf
    else
      conf = ConfigFactory.load() // load application.conf
    conf = conf.getConfig("DT")
    val outputconfig = conf.getConfig("output.ct")
    val pipe = outputconfig.getStringList("pipeline").toArray

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    def writeIfExists[T <: Any](conf_path:String, ds:DataSet[CT2[T]], stringfun:((CT2[T]) => String) = ((ct2:CT2[T]) => ct2.toString)): Unit = {
      if(outputconfig.hasPath(conf_path)){
        val o = ds.map(stringfun).map(Tuple1(_))
        if(outputconfig.getString(conf_path) equals "stdout")
          o.print()
        else{
          o.writeAsCsv(outputconfig.getString(conf_path), "\n", "\t", writeMode = FileSystem.WriteMode.OVERWRITE)
          if(pipe(pipe.size-1) == conf_path) {
            env.execute("CtDT")
            System.exit(0)
            return
          }
        }
      }
    }

    // get input data
    val in = conf.getString("input.text")

    var text:DataSet[String] = null
    if(new File(in).exists)
      text = env.readTextFile(in)
    else
      text = env.fromCollection(in.split('\n'))

    val ct_raw = text
      .filter(_ != null)
      .filter(!_.trim().isEmpty())
      .map(TextToCT2.ngram_patterns(_,5,3))
      .flatMap(s => Util.collapse(s))

    writeIfExists("raw", ct_raw)

    val ct_accumulated = ct_raw.groupBy("A","B")
      .sum("n11")
      .filter(_.n11 > 1)

    writeIfExists("accAB", ct_accumulated)

    val ct_accumulated_A = ct_raw.map(ct => {ct.n1dot=ct.n11; ct})
      .groupBy("A")
      .reduce((x,y) => x.copy(n11 = x.n1dot+y.n1dot))
      .map(ct => {ct.B = "@"; ct})
      .filter(_.ndot1 > 1)

    writeIfExists("accA", ct_accumulated_A)

    val ct_accumulated_B = ct_raw.map(ct => {ct.ndot1 = ct.n11; ct})
      .groupBy("B")
      .reduce((x,y) => x.copy(ndot1 = x.ndot1 + y.ndot1))
      .map(ct => {ct.A = "@"; ct})
      .filter(ct => ct.ndot1 > 1)

    writeIfExists("accB", ct_accumulated_B)

    val n = ct_accumulated.map(ct => ct.n11).reduce(_+_).collect()(0);
    println(n)

    val ct_all = ct_accumulated
      .joinWithHuge(ct_accumulated_A)
      .where("A")
      .equalTo("A")((x, y) => { x.n1dot = y.n1dot; x })
      .joinWithHuge(ct_accumulated_B)
      .where("B")
      .equalTo("B")((x, y) => { x.ndot1 = y.ndot1; x })
      .map(ct => {ct.n = n; ct.n11 = ct.lmi(); ct})

    writeIfExists("accall", ct_all)

    val ct_all_filtered = ct_all.filter(ct => ct.ndot1 > 1 && ct.ndot1 <= 1000)
      .groupBy("A")
      .sortGroup("n11", Order.DESCENDING)
      .first(1000)

    val joined = ct_all_filtered
      .joinWithHuge(ct_all_filtered)
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
      .joinWithHuge(dtf)
      .where("A").equalTo("A")((x, y) => { x.n1dot = y.n1dot; x })
      .groupBy("A")
      .sortGroup("n11", Order.DESCENDING)
      .first(100)

    writeIfExists("dt", dtsort)

  }
}