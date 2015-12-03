package de.tudarmstadt.lt.flinkdt.tasks

import java.io.File

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.flink.api.scala._


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

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = config_dt.getString("input.text")

  val ds = {

    DSReader(in,env) ~>
//
    Extractor() ~> DSWriter(new File(outputbasedir, outputconfig.getString("raw")).getAbsolutePath) ~>
//
    N11Sum() ~> DSWriter(new File(outputbasedir, outputconfig.getString("accAB")).getAbsolutePath) ~>
//
    WhiteListFilter(if(config_dt.hasPath("input.whitelist")) config_dt.getString("input.whitelist") else null, env) ~>
//
    ComputeCT2() ~> DSWriter(new File(outputbasedir, outputconfig.getString("accall")).getAbsolutePath) ~>
//
    ComputeDT.fromCT2() ~>
//
    FilterSortDT.CT2Min() ~> DSWriter(new File(outputbasedir, outputconfig.getString("dt")).getAbsolutePath)

  }.process()

  env.execute(jobname)

}
