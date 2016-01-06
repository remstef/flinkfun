package de.tudarmstadt.lt.flinkdt

import _root_.java.lang

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import de.tudarmstadt.lt.flinkdt.types.{CT2def}
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by Steffen Remus
  */
object CtGraphDText extends App {

  case class AdjacencyList[T1, T2](source: CT2def[T1, T2], targets: Array[CT2def[T1, T2]])

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

  def writeIfExists[T1 <: Any, T2 <: Any](conf_path:String, ds:DataSet[CT2def[T1, T2]], stringfun:((CT2def[T1, T2]) => String) = ((ct2:CT2def[T1, T2]) => ct2.toString)): Unit = {
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

  val ct_raw:DataSet[CT2def[String,String]] = text
    .filter(_ != null)
    .filter(!_.trim().isEmpty())
    .flatMap(s => Util.collapseCT2(TextToCT2.ngram_patterns(s,5,3).map(_.asCT2Full())).flatMap(c => Seq(c, c.flipped())))
    .groupBy("a","b")
    .sum("n11")
    .filter(_.n11 > 1)

  val n = Try(ct_raw.map(ct => ct.n11).reduce(_+_).collect()(0) / 2f).getOrElse(0f)
  println(n)

  val adjacencyLists = ct_raw
    .groupBy("a","isflipped")
    .reduceGroup(new GroupReduceFunction[CT2def[String,String], AdjacencyList[String, String]]() {
      override def reduce(values: lang.Iterable[CT2def[String,String]], out: Collector[AdjacencyList[String, String]]): Unit = {
        val temp:CT2def[String,String] = CT2def(null,null, n11 = 0, ndot1 = 0, n1dot = 0, n = 0)
        val l = values.asScala
          .map(t => {
            temp.a = t.a
            temp.n11 += t.n11
            temp.n1dot += t.n11
            t })
          .map(t => {
            t.n = n
            if(t.isflipped){
              t.a = t.b
              t.b = temp.a
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
    .groupBy("a","b")
    .reduceGroup(new GroupReduceFunction[CT2def[String,String], CT2def[String,String]]() {
      override def reduce(values: lang.Iterable[CT2def[String,String]], out: Collector[CT2def[String,String]]): Unit = {
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
