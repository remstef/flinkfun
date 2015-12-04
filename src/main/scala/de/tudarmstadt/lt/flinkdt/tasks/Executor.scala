package de.tudarmstadt.lt.flinkdt.tasks

import java.io.File

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.flink.api.scala._


/**
  * Created by Steffen Remus
  */
object Executor extends App {

  val jobname = getClass.getSimpleName.replaceAllLiterally("$","")
  DSTaskConfig.load(args, jobname=jobname)

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  // get input data
  val in = DSTaskConfig.in_text

  val ds = {

    Extractor() ~> DSWriter(DSTaskConfig.out_raw) ~>
//
    N11Sum() ~> DSWriter(DSTaskConfig.out_accumulated_AB) ~>
//
    WhiteListFilter(DSTaskConfig.in_whitelist, env) ~>
//
    ComputeCT2() ~> DSWriter(DSTaskConfig.out_accumulated_CT) ~>
//
    ComputeDT.fromCT2() ~>
//
    FilterSortDT.CT2Min_CT2() ~> DSWriter(DSTaskConfig.out_dt_sorted)

  }.process(env,in)

  env.execute(jobname)

}
