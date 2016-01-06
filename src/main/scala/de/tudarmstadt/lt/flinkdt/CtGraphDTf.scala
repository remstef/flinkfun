package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import de.tudarmstadt.lt.flinkdt.types.{CT2def}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

/**
  * Created by Steffen Remus
  */
object CtGraphDTf extends App {

  val config:Config =
    if(args.length > 0)
      ConfigFactory.parseFile(new File(args(0))).withFallback(ConfigFactory.load()).resolve() // load conf with fallback to default application.conf
    else
      ConfigFactory.load() // load default application.conf

  val jobname = getClass.getSimpleName.replaceAllLiterally("$","")
  val config_dt = config.getConfig("DT")
  val outputconfig = config_dt.getConfig("output.ct")
  val outputbasedir = new File(if(config_dt.hasPath("output.basedir")) config_dt.getString("output.basedir") else "./", s"out-${jobname}")
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

  val ct_accumulated:DataSet[CT2def[String, String]] = text
    .filter(_ != null)
    .filter(!_.trim().isEmpty())
    .flatMap(s => TextToCT2.ngram_patterns(s,5,3))
    .groupBy("a","b")
    .sum("n11")
    .filter(_.n11 > 1)
    .map(_.asCT2Full())

  writeIfExists("accAB", ct_accumulated)

  val n = ct_accumulated.map(ct => ct.n11).reduce(_+_)
  val ct_accumulated_n = ct_accumulated.crossWithTiny(n)((ct,n) => {ct.n = n; ct})

  val adjacencyLists = ct_accumulated_n
    .groupBy("a")
    .reduceGroup((iter, out:Collector[CT2def[String, String]]) => {
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

  writeIfExists("accA", adjacencyLists)

  val descending_ordering = new Ordering[(CT2def[String,String],Float)] {
    def compare(o1:(CT2def[String,String], Float), o2:(CT2def[String,String], Float)): Int = {
      -o1._2.compareTo(o2._2)
    }
  }

  val adjacencyListsRev = adjacencyLists
    .groupBy("b")
    .reduceGroup((iter, out:Collector[CT2def[String, String]]) => {
      val temp:CT2def[String,String] = CT2def(null,null, n11 = 0, n1dot = 0, ndot1 = 0)
      val l = iter.toSeq
      l.foreach(t => {
        temp.b = t.b
        temp.n11 += t.n11
        temp.ndot1 += t.n11
      })

//      val s:FixedSizeTreeSet[(CT2[String,String],Float)] = FixedSizeTreeSet.empty(descending_ordering,1000)
//      var wc = 0
//      l.foreach(t => {
//        t.ndot1 = temp.ndot1
//        wc += 1
//        val tup = (t, t.lmi())
//        s += tup
//      })
//
//      if(wc > 1 && wc <= 1000) {
//        s.foreach(ct_x => {
//          s.foreach(ct_y => {
//            if (ct_y._2 > 0)
//              out.collect(CT2(ct_x._1.a, ct_y._1.a, 1f))
//          })
//        })
//      }


      val ll = l
        .map(t => {
          t.ndot1 = temp.ndot1
          t })
        .map(t => (t, t.lmi()))

      val wc = ll.count(x => true)
      if(wc > 1 && wc <= 1000) {
        val lll = ll.sortBy(-_._2).take(1000) // sort descending
     //   lll.foreach(println(_))
        lll.foreach(ct_x => {
          lll.foreach(ct_y => {
            if (ct_y._2 > 0)
              out.collect(CT2def(ct_x._1.a, ct_y._1.a, 1f))
          })
        })
      }

    })

  val dt = adjacencyListsRev
    .groupBy("a","b")
    .sum("n11")
    // evrything from here is from CtDT and can be optimized
    .filter(_.n11 > 1)

  val dtf = dt
    .groupBy("a")
    .sum("n1dot")
    .filter(_.n1dot > 2)

  val dtsort = dt
    .join(dtf)
    .where("a").equalTo("a")((x, y) => { x.n1dot = y.n1dot; x })
    .groupBy("a")
    .sortGroup("n11", Order.DESCENDING)
    .first(200)

  writeIfExists("dt", dtsort)

}
