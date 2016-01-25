///
// make script compilable
///
//import org.apache.flink.api.scala.ExecutionEnvironment
//import org.apache.flink.api.scala._
//val env = ExecutionEnvironment.getExecutionEnvironment
//val d = env.readTextFile("")

///
// convenience: import all flinkdt resources
///
import de.tudarmstadt.lt.flinkdt.textutils._
import de.tudarmstadt.lt.flinkdt.types._
import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.pipes._
import de.tudarmstadt.lt.flinkdt.examples._

///
// set parallelism to maximum
///
env.setParallelism(Int.MaxValue)

///
// useful commands
///
//val ct = d.map(CtFromString[CT2red[String,String],String,String](_))
//val ct = d.map(CtFromString[CT2def[String,String],String,String](_))
//val ct = d.map(CtFromString[CT2ext[String,String],String,String](_))
//ct.first(1).map(_.prettyPrint).print()

// filter
// ct.filter(_.a == "xyz")

// sort globally by arbitrary value
// ct.map(c => (1, c.lmi_n, c)).groupBy(0).sortGroup(1, Order.DESCENDING).map(_._3)
