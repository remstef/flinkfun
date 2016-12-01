package de.uhh.lt.flink

import org.apache.flink.api.scala._
import de.tudarmstadt.lt.scalautils.SimpleArgParse
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation

object JoinJBC {
  
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

      case class t (j:String, b:String, v:Double)
        
			// read a DataSet from an external source
			val ds: DataSet[t] = env.readCsvFile(in, fieldDelimiter="\t")
			
      val grpd = ds.coGroup(ds).where("b").equalTo("b") // here we get a tuple of two possibly huge arrays
      val jnd = grpd.flatMap { tups => for(tup1 <- tups._1; tup2 <- tups._2) yield t(tup1.j, tup2.j, tup1.v) }
      val red = jnd.groupBy("j","b").sum("v")
              			
	    red.writeAsCsv(out, fieldDelimiter="\t")
	    
	    env.execute()
	    
	}



}