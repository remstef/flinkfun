package de.tudarmstadt.lt.flinkdt

import java.io.File
import java.lang.Iterable

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Created by Steffen Remus
  */
object CtGraphDTf extends App {

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
    .flatMap(s => Util.collapse(TextToCT2.ngram_patterns(s,5,3)).flatMap(c => Seq(c, c.flipped())))
    .groupBy("A","B")
    .sum("n11")
    .filter(_.n11 > 1)

  val adjacencyLists = ct_raw
    .groupBy("A","isflipped")
    .reduceGroup(new GroupReduceFunction[CT2[String], AdjacencyList[String]]() {
      override def reduce(values: Iterable[CT2[String]], out: Collector[AdjacencyList[String]]): Unit = {
        val temp:CT2[String] = CT2(null,null, n11 = 0, ndot1 = 0, n1dot = 0, n = 0)
        val l = values.asScala
          .map(t => {
            temp.A = t.A
            temp.n11 += t.n11
            temp.n1dot += t.n11
            t })
          .map(t => {
            if(t.isflipped){
              t.A = t.B
              t.B = temp.A
              t.ndot1 = temp.n1dot
            }else{
              t.n1dot = temp.n1dot
            }
            t
          })
        out.collect(AdjacencyList(temp, l.toArray))
      }
    })

  val a = adjacencyLists.flatMap(_.targets)
    .groupBy("A","B")
    .reduceGroup(new GroupReduceFunction[CT2[String], CT2[String]]() {
      override def reduce(values: Iterable[CT2[String]], out: Collector[CT2[String]]): Unit = {
        val s = values.asScala.toSeq
        assert(s.length == 2)
        assert(s(0).isflipped ^ s(1).isflipped)
        assert(s(0).n11 == s(1).n11)
        val flipped_index = if(s(0).isflipped) 0 else 1
        val orig = s((flipped_index+1)%2)
        orig.ndot1 = s(flipped_index).ndot1
        orig.n = 0
        out.collect(orig)
      }
    })


  writeIfExists("accall",a)

  env.execute()
}
