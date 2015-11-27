package de.tudarmstadt.lt.flinkdt

import java.io.File
import java.lang.Iterable

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.common.operators.Order

import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import org.apache.flink.api.scala._

import scala.util.Try

/**
  * Created by Steffen Remus
  */
object CtGraphDT extends App {

  case class AdjacencyList[T](source: CT2[T], targets: Array[CT2[T]])

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

  def writeIfExists[T <: Any](conf_path:String, ds:DataSet[CT2[T]], stringfun:((CT2[T]) => String) = ((ct2:CT2[T]) => ct2.toString)): Unit = {
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

  val ct_raw:DataSet[CT2[String]] = text
    .filter(_ != null)
    .filter(!_.trim().isEmpty())
    .flatMap(s => Util.collapse(TextToCT2.ngram_patterns(s,5,3)))
    .groupBy("A","B")
    .sum("n11")
    .filter(_.n11 > 1)
    .map(c => {c.n = 0; c}) // set n to zero, it would be wrong anyways

//  val n = Try(ct_raw.map(ct => ct.n11).reduce(_+_).collect()(0)).getOrElse(0f)
//  println(n)
//
//  val adjacencyLists = ct_raw
//    .groupBy("A")
//    .reduceGroup(new GroupReduceFunction[CT2[String], AdjacencyList[String]]() {
//      override def reduce(values: Iterable[CT2[String]], out: Collector[AdjacencyList[String]]): Unit = {
//        val temp:CT2[String] = CT2(null,null, n11 = 0, ndot1 = 0, n1dot = 0, n = 0)
//        val l = values.asScala
//          .map(t => {
//            temp.A = t.A
//            temp.n11 += t.n11
//            temp.n1dot += t.n11
//            t })
//          .map(t => {
//            t.n = n
//            t.n1dot = temp.n1dot
//            t })
//        // TODO: filter by too many contexts
//        out.collect(AdjacencyList(temp, l.toArray))
//      }
//    })

//  val adjacencyListsRev = adjacencyLists.flatMap(_.targets)
    val adjacencyListsRev = ct_raw
    .groupBy("B")
    .reduceGroup(new GroupReduceFunction[CT2[String], AdjacencyList[String]]() {
      override def reduce(values: Iterable[CT2[String]], out: Collector[AdjacencyList[String]]): Unit = {
        val temp:CT2[String] = CT2(null,null, n11 = 0, n1dot = 0, ndot1 = 0, n = 0)
        val l = values.asScala
          .map(t => {
            temp.B = t.B
            temp.n11 += t.n11
            temp.ndot1 += t.n11
            t })
          .map(t => {
            t.ndot1 = temp.ndot1
            t })
          // TODO: filter by pmi, ndot1, and so on
        val ll = l.flatMap(ct_x => l.map(ct_y => CT2(ct_x.A, ct_y.A)).toSeq) // this could by optimized due to symmetry
        out.collect(AdjacencyList(temp, ll.toArray))
      }
    })

  val dt = adjacencyListsRev.flatMap(_.targets)
    .groupBy("A","B")
    .sum("n11")
// evrything from here is from CtDT and can be optimized
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


//  env.execute()
}
