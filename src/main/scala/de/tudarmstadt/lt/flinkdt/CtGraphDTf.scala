package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import de.tudarmstadt.lt.scalautils.{FormatUtils, FixedSizeTreeSet}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Try

/**
  * Created by Steffen Remus
  */
object CtGraphDTf extends App {

  val config:Config =
    if(args.length > 0)
      ConfigFactory.parseFile(new File(args(0))).withFallback(ConfigFactory.load()).resolve() // load conf with fallback to default application.conf
    else
      ConfigFactory.load() // load default application.conf

  val jobname = s"out-${getClass.getSimpleName.replaceAllLiterally("$","")}"
  val config_dt = config.getConfig("DT")
  val outputconfig = config_dt.getConfig("output.ct")
  val outputbasedir = new File(if(config_dt.hasPath("output.basedir")) config_dt.getString("output.basedir") else "./", jobname)
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
          env.execute(jobname)
        }
      }
    }
  }

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = config_dt.getString("input.text")

  val text:DataSet[String] = if(new File(in).exists) env.readTextFile(in) else env.fromCollection(in.split('\n'))

  val ct_accumulated:DataSet[CT2[String, String]] = text
    .filter(_ != null)
    .filter(!_.trim().isEmpty())
    .flatMap(s => TextToCT2.ngram_patterns(s,5,3))
    .groupBy("A","B")
    .sum("n11")
    .filter(_.n11 > 1)
    .map(_.toCT2())

  writeIfExists("accAB", ct_accumulated)

  val temp_n = ct_accumulated.map(ct => ct.n11).reduce(_+_)
  temp_n.map(i => Tuple1(s"N=${FormatUtils.format(i)}")).print()
  val n = Try(temp_n.collect()(0)).getOrElse(0f)
  println(s"N=${FormatUtils.format(n)}")

  val adjacencyLists = ct_accumulated
    .groupBy("A")
    .reduceGroup((iter, out:Collector[CT2[String, String]]) => {
      var n1dot:Float = 0f
      val l = iter.toIterable
      l.foreach(t => n1dot += t.n11)

      if(n1dot > 1) {
        l.foreach(t => {
          t.n1dot = n1dot
          out.collect(t)
        })
      }
    })
    .map(ct => {ct.n = n; ct})

  writeIfExists("accA", adjacencyLists)

//  val descending_ordering = new Ordering[CT2[String,String]] {
//    def compare(o1:CT2[String,String], o2:CT2[String,String]): Int = {
//      val r = o1.lmi().compareTo(o2.lmi())
//      if(r != 0)
//        return r
//      return (o1.lmi().compareTo(o2.lmi()))
//    }
//  }

  val adjacencyListsRev = adjacencyLists
    .groupBy("B")
    .reduceGroup((iter, out:Collector[CT2[String, String]]) => {
      val temp:CT2[String,String] = CT2(null,null, n11 = 0, n1dot = 0, ndot1 = 0, n = 0)
      val l = iter.toSeq
      l.foreach(t => {
        temp.B = t.B
        temp.n11 += t.n11
        temp.ndot1 += t.n11
      })
      val ll = l
        .map(t => {
          t.ndot1 = temp.ndot1
          t })
        .map(t => (t, t.lmi()))

      val wc = ll.count(x => true)
      if(wc > 1 && wc <= 1000) {
        val lll = ll.sortBy(-_._2).take(1000) // sort descending
        lll.foreach(ct_x => {
          lll.foreach(ct_y => {
            if (ct_y._2 > 0)
              out.collect(CT2(ct_x._1.A, ct_y._1.A, 1f))
          })
        })
      }
    })

  val dt = adjacencyListsRev
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
    .first(200)

  writeIfExists("dt", dtsort)

}
