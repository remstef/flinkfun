package de.uhh.lt.flink

import org.apache.flink.api.scala._
import de.tudarmstadt.lt.scalautils.SimpleArgParse
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation

object JoinJBD {
  
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

      type t = (String, String)
        
			// read a DataSet from an external source
			val ds: DataSet[t] = env.readCsvFile(in, fieldDelimiter="\t")
			
      val jnd = ds
        .join(ds).where("_2").equalTo("_2") { (l,r) => (l._1, r._1, 1l) }
        .groupBy("_1","_2")
			  .sum("_3")
			
	    jnd.writeAsCsv(out, fieldDelimiter="\t")
	    
	    env.execute()
	    
	}



}