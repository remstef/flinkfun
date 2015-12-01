package de.tudarmstadt.lt.flinkdt.tasks

import java.io.File

import com.typesafe.config.{ConfigFactory, Config}
import de.tudarmstadt.lt.flinkdt.CT2Min
import de.tudarmstadt.lt.flinkdt.CtGraphDT._
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

import scala.collection.mutable

/**
  * Created by Steffen Remus
  */
object Executor extends App {

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

  def writeIfExists[T1 <: Any, T2 <: Any](conf_path:String, ds:DataSet[CT2Min[T1, T2]], stringfun:((CT2Min[T1, T2]) => String) = ((ct2:CT2Min[T1, T2]) => ct2.toString)): Unit = {
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


  val e1:DtTask[String, CT2Min[String,String]] = new Extractor()
  val e2:DtTask[CT2Min[String,String],CT2Min[String,String]] = new N11Sum[String,String,CT2Min[String,String]]()

  val ds = e1.fromLines(text)
  var ds1 = e1.process(ds)
  val ds2 = e2.process(ds1)

//  val pipeline = Seq()
//  val ds = pipeline(0).fromLines(text)

//  for(e <- pipeline){
//    ds = e.process(ds)
//  }





}
