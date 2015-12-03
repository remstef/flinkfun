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
  val in = DSTaskConfig.input

  val ds = {

    Extractor() ~> DSWriter(DSTaskConfig.raw_output) ~>
//
    N11Sum() ~> DSWriter(DSTaskConfig.accumulated_AB_output) ~>
//
    WhiteListFilter(DSTaskConfig.input_whitelist, env) ~>
//
    ComputeCT2() ~> DSWriter(DSTaskConfig.accumulated_CT_output) ~>
//
    ComputeDT.fromCT2() ~>
//
    FilterSortDT.CT2Min_CT2() ~> DSWriter(DSTaskConfig.dt_sorted_output)

  }.process(env,in)

  env.execute(jobname)

}
