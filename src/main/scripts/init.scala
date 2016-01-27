///
// make script compilable (for development purposes)
///
import org.apache.flink.api.scala._
val env = ExecutionEnvironment.getExecutionEnvironment
val d = env.readTextFile("")

///
// convenience: import all flinkdt resources
///
import sys.process._
import de.tudarmstadt.lt.flinkdt.textutils._
import de.tudarmstadt.lt.flinkdt.types._
import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.pipes._
import de.tudarmstadt.lt.flinkdt.examples._

///
// other necessary imports
///
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.reflect.ClassTag

///
// convenience: define some implicit functions on CT2 objects
///
def readCT2[C <: CT2 : ClassTag : TypeInformation](in:String) : DataSet[C] = env.readTextFile(in).map(CtFromString[C, String, String](_))
println("defined readCT2(\"path\")")

implicit def ct2_ext_computation(x: DataSet[CT2red[String,String]]) = new {
  def computeCT2ext:DataSet[CT2ext[String,String]] =
    ComputeCT2[CT2red[String, String], CT2ext[String, String], String, String]().process(x)
}
println("defined ct2red.computeCT2ext")

implicit def dt_computation(x: DataSet[CT2ext[String,String]]) = new {
  def computeDT(prune: Boolean = false):DataSet[CT2red[String,String]] = {
    val p = ComputeDTSimplified.byJoin[CT2ext[String, String], String, String]()
    if(prune)
      { Prune[String, String](sigfun = _.lmi_n, Order.ASCENDING) ~> p }.process(x)
    else
      p.process(x)
  }
}
println("defined ct2ext.computeDT(prune=false)")

implicit def ct2_get_top[CT <: CT2 : ClassTag : TypeInformation](x: DataSet[CT]) = new {
  def topN(n:Int, valfun:CT => Float = _.n11, order:Order = Order.DESCENDING):DataSet[CT] = {
    val ds_first_n = x
      .map(ct => (ct, valfun(ct))) // apply valfun
      .groupBy("_1.a")
      .sortGroup(1, order)
      .first(n)
      .map(_._1)
    ds_first_n
  }
}
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