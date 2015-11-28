package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

/**
  * Created by Steffen Remus
  */
object CtGraphDT extends App {

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

  def writeIfExists[T1 <: Any, T2 <: Any](conf_path:String, ds:DataSet[CT2[T1, T2]], stringfun:((CT2[T1, T2]) => String) = ((ct2:CT2[T1, T2]) => ct2.toString)): Unit = {
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
    .flatMap(s => TextToCT2.ngram_patterns(s,5,3))

  val mapStringCtToInt = ct_raw.map(ct => {
    val id_A:Int = ct.A.hashCode
    val id_B:Int = ct.B.hashCode
    val newct = CT2(id_A, id_B, ct.n11, ct.n1dot, ct.ndot1, ct.n)
    (newct, Seq((id_A, ct.A), (id_B, ct.B)))
  })

  val id2string = mapStringCtToInt.map(_._2).flatMap(l => l).distinct(0)

  val ct_raw_int = mapStringCtToInt.map(_._1)

  val ctagg = ct_raw_int
    .groupBy("A","B")
    .sum("n11")
    .filter(_.n11 > 1)
    .map(c => {c.n = 0; c}) // set n to zero, it would be wrong anyways

  val adjacencyListsRev = ctagg
    .groupBy("B")
    .reduceGroup((iter, out:Collector[CT2[Int, Int]]) => {
      val temp:CT2[Int, Int] = CT2(0, 0, n11 = 0, n1dot = 0, ndot1 = 0, n = 0)
      val l = iter
        .map(t => {
          temp.B = t.B
          temp.n11 += t.n11
          temp.ndot1 += t.n11
          t })
        .map(t => {
          t.ndot1 = temp.ndot1
          t }).toIterable
      // TODO: might be a bottleneck, it creates multiple new sequences (one new sequence per each entry)
      l.foreach(ct_x => l.foreach(ct_y => out.collect(CT2(ct_x.A, ct_y.A)))) // this could by optimized due to symmetry
    })

    val dt_int = adjacencyListsRev
      .groupBy("A","B")
      .sum("n11")

    val dt = dt_int
      .join(id2string).where("A").equalTo(0)((ct,tup) => (ct, tup._2))
      .join(id2string).where("_1.B").equalTo(0)((ct_tup,tup) => CT2[String,String](ct_tup._2, tup._2, ct_tup._1.n11))

    writeIfExists("dt", dt)

//      // evrything from here is from CtDT and can be optimized
//      .filter(_.n11 > 1)
//
//  val dtf = dt
//    .groupBy("A")
//    .sum("n1dot")
//    .filter(_.n1dot > 2)
//
//  val dtsort = dt
//    .join(dtf)
//    .where("A").equalTo("A")((x, y) => { x.n1dot = y.n1dot; x })
//    .groupBy("A")
//    .sortGroup("n11", Order.DESCENDING)
//    .first(10)
//
//  writeIfExists("dt", dtsort)

}
