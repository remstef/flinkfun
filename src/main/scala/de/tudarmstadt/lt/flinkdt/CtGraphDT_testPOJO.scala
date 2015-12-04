package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import de.tudarmstadt.lt.flinkdt.CT2MinJ
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

/**
  * Created by Steffen Remus
  */
object CtGraphDT_testPOJO extends App {

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

  def writeIfExists(conf_path:String, ds:DataSet[CT2MinJ[String,String]], stringfun:((CT2MinJ[String,String]) => String) = ((ct2:CT2MinJ[String,String]) => ct2.toString)): Unit = {
    if(outputconfig.hasPath(conf_path)){
      val o = ds.map(stringfun).map(Tuple1(_))
      if(outputconfig.getString(conf_path) equals "stdout") {
        o.print()
      }
      else{
        o.writeAsCsv(new File(outputbasedir, outputconfig.getString(conf_path)).getAbsolutePath, "\n", "\t", writeMode = FileSystem.WriteMode.OVERWRITE)
        if(pipe(pipe.size-1) == conf_path) {
          env.execute(jobname)
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

  val ct_raw:DataSet[CT2MinJ[String,String]] = text
    .filter(_ != null)
    .filter(!_.trim().isEmpty())
    .flatMap(s => TextToCT2.ngram_patterns(s,5,3).map(ct => {val r = new CT2MinJ[String,String](); r.A = ct.a; r.B = ct.b; r.n11 = ct.n11; r}))


  val ctagg = ct_raw
    .groupBy("A","B")
    .reduce((l,r) => {l.n11 += r.n11; l})
    .filter(_.n11 > 1)

  val adjacencyListsRev = ctagg
    .groupBy("B")
    .reduceGroup((iter, out:Collector[CT2MinJ[String,String]]) => {
      val l = iter.map(_.A).toIterable
      // TODO: might be a bottleneck, it creates multiple new sequences (one new sequence per each entry)
      l.foreach(a => l.foreach(b => {val r = new CT2MinJ[String,String](); r.A=a; r.B=b; r.n11=1f; out.collect(r)})) // this could by optimized due to symmetry
    })

  val dt = adjacencyListsRev
    .groupBy("A","B")
    .reduce((l,r) => {l.n11 += r.n11; l})

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
