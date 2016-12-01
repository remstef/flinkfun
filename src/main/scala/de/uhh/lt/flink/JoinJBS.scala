package de.uhh.lt.flink

import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import de.tudarmstadt.lt.scalautils.SimpleArgParse
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object JoinJBS {
  
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
			val ds: DataSet[(String, String, Double)] = env.readCsvFile(in, fieldDelimiter="\t")
			// register the DataSet under the name "jb" with the fields j, b, and v
			tEnv.registerDataSet("jb1", ds, 'j, 'b, 'v)
			
			val ds2: DataSet[(String, String, Double)] = env.readCsvFile(in, fieldDelimiter="\t")
			// register the DataSet under the name "jb" with the fields j, b, and v
			tEnv.registerDataSet("jb2", ds, 'j, 'b, 'v)

			// run a SQL query on the Table and retrieve the result as a new Table
			val jnd = tEnv.sql {"""
SELECT jb1.j, jb1.j, sum(jb1.v) 
FROM jb1 JOIN jb2 ON jb1.b
GROUP BY jb1.j, jb2.j
"""}

	    jnd.toDataSet[(String, String, Double)].writeAsCsv(out, fieldDelimiter="\t")
	    
	    env.execute()
	    
	}



}