import de.tudarmstadt.lt.flinkdt.textutils._
import de.tudarmstadt.lt.flinkdt.types._
import org.apache.flink.api.scala.ExecutionEnvironment


// import org.apache.flink.api.scala._
//val env = ExecutionEnvironment.getExecutionEnvironment
//val ds = env.readTextFile("")

val ct = ds.map(CtFromString[CT2def[String,String],String,String](_))
ct.map(_.prettyPrint()).print()

