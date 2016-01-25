///
// convenience: import all flinkdt resources
///
import de.tudarmstadt.lt.flinkdt.textutils._
import de.tudarmstadt.lt.flinkdt.types._
import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.pipes._
import de.tudarmstadt.lt.flinkdt.examples._

//import org.apache.flink.api.scala.ExecutionEnvironment
//import org.apache.flink.api.scala._
//val env = ExecutionEnvironment.getExecutionEnvironment
//val d = env.readTextFile("")

val ct = d.map(CtFromString[CT2def[String,String],String,String](_))
ct.map(_.prettyPrint()).print()

