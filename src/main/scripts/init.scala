///
// make script compilable (for development purposes)
///
//import org.apache.flink.api.scala._
//val env = ExecutionEnvironment.getExecutionEnvironment
//val d = env.readTextFile("")

///
// convenience: import all flinkdt resources
///
import org.apache.flink.api.scala.DataSet

import sys.process._
import de.tudarmstadt.lt.flinkdt.textutils._
import de.tudarmstadt.lt.flinkdt.types._
import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.pipes._
import de.tudarmstadt.lt.flinkdt.examples._
import de.tudarmstadt.lt.flinkdt.Implicits._
///
// print some information about available implicit functions
///
println("defined env.readCT2r(\"path\")")
println("defined env.readCT2d(\"path\")")
println("defined env.readCT2e(\"path\")")
println("defined ct2red.computeCT2ext")
println("defined ct2ext.computeDT(prune=false)")
println("defined ct.topN(n, valfun=_.n11, order=Order.DESCENDING)")

///
// set parallelism to maximum
///
env.setParallelism(Int.MaxValue)
println("increased parallelism to maximum")

///
// useful commands
///

// read extracted data, compute complete ct2, compute dt, sort dt entries
// val ct = readCT2(path)
// val ctext = ct.computeCT2ext()
// val dt = ctext.computeDT(prune = true)
// val dtf = dt.topN(10)
// dtf.first(20).print


//val ct = d.map(CtFromString[CT2red[String,String],String,String](_))
//val ct = d.map(CtFromString[CT2def[String,String],String,String](_))
//val ct = d.map(CtFromString[CT2ext[String,String],String,String](_))

// prettyprint
//ct.first(1).map(_.prettyPrint).print()

// filter
// ct.filter(_.a == "xyz")

// sort globally by arbitrary value, e.g. top 100 by lmi
// ct.map(c => (1, c.lmi_n, c)).filter(_._2 >= 0).groupBy(0).sortGroup(1, Order.ASCENDING).first(100).map(_._3)

// compute CT2
//val ctp = ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]().process(ct)

// prune
// ctp.filter(_.n11 >= DSTaskConfig.param_min_n11).filter(_.n1dot >= DSTaskConfig.param_min_n1dot).filter(ct => ct.odot1 <= DSTaskConfig.param_max_odot1 && ct.odot1 >= DSTaskConfig.param_min_odot1)
// ctp.map(ct => (ct, sigfun(ct))).filter(_._2 >= DSTaskConfig.param_min_sig).groupBy("_1.a").sortGroup("_2", order).first(DSTaskConfig.param_topn_sig).map(_._1)

// compute DT
//val dt = ComputeDTSimplified.byJoin[CT2red[String,String],String,String]().process(ct)

// compute pruned DT
//val dt =  { Prune[String, String](sigfun = _.lmi_n, Order.ASCENDING) ~> ComputeDTSimplified.byJoin[CT2red[String,String],String,String]() }.process(ct)

// filter and sort DT
//val dtf = FilterSortDT[CT2red[String,String],String, String](_.n11).process(dt)

// run commandline command
// "ls" !
// "hdfs dfs -du -h " !