package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{Config, ConfigValue, ConfigFactory}
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._


/**
 * Created by Steffen Remus.
 */
object ConfTest {

  val LOG = LoggerFactory.getLogger(ConfTest.getClass)

  def main(args: Array[String]) {

    var conf:Config = null
    if(args.length > 0)
      conf = ConfigFactory.parseFile(new File(args(0))).resolve()
    else
      conf = ConfigFactory.load()

    System.setProperty("DT.question","What?")

    for(entry <- conf.getConfig("DT").entrySet()){
      println(entry.getKey + "\t" + entry.getValue.render() + ":" + entry.getValue.valueType())
    }

    println(conf.getConfig("DT").getConfig("output").hasPath("dt"))

    LOG.trace("this is a trace message")
    LOG.debug("this is a debug message")
    LOG.info("this is a info message")
    LOG.warn("this is a warn message")
    LOG.error("this is a error message")

  }


}
