package de.uhh.lt.flink

import org.apache.flink.api.scala._
import de.tudarmstadt.lt.scalautils.SimpleArgParse
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector

object JoinJBG2 {
  
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

      type t = (String, String, Double)
        
			// read a DataSet from an external source
			val ds: DataSet[t] = env.readCsvFile(in, fieldDelimiter="\t")
			
			val jnd = ds
			  .groupBy("_2")
			  .reduceGroup { (it, col:Collector[t]) =>
			    val s = it.toSeq
			    for(t1 <- s; t2 <- s) 
			      col.collect((t1._1, t2._1, t1._3))
			  }.rebalance()
        .groupBy("_1","_2")
			  .sum("_3")
			

			
	    jnd.writeAsCsv(out, fieldDelimiter="\t")
	    
	    env.execute()
	    
	}



}