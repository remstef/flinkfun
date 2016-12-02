package de.uhh.lt.flink

import org.apache.flink.api.scala._
import de.tudarmstadt.lt.scalautils.SimpleArgParse
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.fs.Path
import org.apache.flink.api.java.io.CsvOutputFormat
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import scala.util.Random
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

object JoinJBX {
  
  val LOG:Logger = LoggerFactory.getLogger(getClass)

	def main(args: Array[String]): Unit = {
    
	    val args_parsed:Map[String, String] = SimpleArgParse(args).map { 
          case (k, v:List[_])  => (k, v.mkString(" "))
          case (k, v) => (k, v.toString)
        }.filter(_._2.nonEmpty)
    
      LOG.info("Arguments: " + args_parsed.toString())
      
	    val in = args_parsed("in")
	    val tmpdir = args_parsed("tmpdir").stripSuffix("/")
	    val out = args_parsed("out")
	    
	    val c = new Configuration
      c.setString("tmpdir", tmpdir)
	  
			val env = ExecutionEnvironment.getExecutionEnvironment
	    args_parsed.get("parallelism").foreach { cores => 
	      LOG.info(s"Setting parallelism to $cores.")
        env.setParallelism(cores.toInt)
      }
      
      type t = (String, String, Double)
        
			// read a DataSet from an external source
			val ds: DataSet[t] = env.readCsvFile(in, fieldDelimiter="\t")
			val paths = ds.groupBy("_2")
			  .reduceGroup { it => 
			    val loc = s"${tmpdir}/1/${Random.nextLong}${Random.nextLong}"
			    val path = new Path(loc)
			    val of = new ScalaCsvOutputFormat[t](path, "\t")
			    of.open(0, 100000)
			    it.foreach { of.writeRecord(_) }
			    of.close()
			    loc
		    }

      val pathloc = s"${tmpdir}/paths"
			paths.writeAsText(pathloc)
			env.execute()

			val path_as_local_list = paths.collect()
			path_as_local_list.zipWithIndex.seq.foreach { case (p, i) => 
			  env.startNewSession()
			  val jnd: DataSet[(String, String, Double)] = env.readCsvFile(p, fieldDelimiter="\t")
			  
			  val crsd:DataSet[(String, String, Double)] = jnd
			    .rebalance()
			    .crossWithHuge[(String, String, Double)](ds){ (t1,t2) => (t1._1, t2._1, t1._3) }
			  
			  val loc = s"${tmpdir}/2/${Random.nextLong}${Random.nextLong}"
			  
			  crsd.writeAsCsv(loc, fieldDelimiter="\t")
			  env.execute(s"${i+1}/${path_as_local_list.size}")

			}

			val parameters:Configuration = new Configuration();
      parameters.setBoolean("recursive.file.enumeration", true);
			val mega_jnd: DataSet[t] = env.readCsvFile[t](s"$tmpdir/2", fieldDelimiter="\t").withParameters(parameters).rebalance()

      val red = mega_jnd
        .groupBy("_1","_2")
			  .sum("_3")
			
	    red.writeAsCsv(out, fieldDelimiter="\t")
	    
	    
	    env.execute()
	    
	    
	    
	}



}