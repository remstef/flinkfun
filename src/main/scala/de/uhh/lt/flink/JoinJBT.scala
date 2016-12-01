package de.uhh.lt.flink

import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import de.tudarmstadt.lt.scalautils.SimpleArgParse
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object JoinJBT {
  
  val LOG:Logger = LoggerFactory.getLogger(getClass)

	def main(args: Array[String]): Unit = {
    
	    val args_parsed:Map[String, String] = SimpleArgParse(args).map { 
          case (k, v:List[_])  => (k, v.mkString(" "))
          case (k, v) => (k, v.toString)
        }.filter(_._2.nonEmpty)
    
      LOG.info("Arguments: " + args_parsed.toString())
      
	    val in = args_parsed("in")
	    val out = args_parsed("out")
	  
			val env = ExecutionEnvironment.getExecutionEnvironment
	    args_parsed.get("parallelism").foreach { cores => 
	      LOG.info(s"Setting parallelism to $cores.")
        env.setParallelism(cores.toInt)
      }
    	val tEnv = TableEnvironment.getTableEnvironment(env)
		    	
			// read a DataSet from an external source
			val ds1: DataSet[(String, String, Double)] = env.readCsvFile(in, fieldDelimiter="\t")
			
			
			val expr1 = ds1.toTable(tEnv)
			
      val jnd = expr1.as('j1, 'b1, 'v1)
        .join(expr1)
        .where("b1 = _2")
        .select('j1, '_1, 'b1, 'v1, '_3)
        .as('j1, 'j2, 'b, 'v1 ,'v2)
        .groupBy('j1, 'j2)
        .select('j1, 'j2, 'v1.sum)

	    jnd.toDataSet[(String, String, Double)].writeAsCsv(out, fieldDelimiter="\t")
	    
	    env.execute()
	    
	}



}