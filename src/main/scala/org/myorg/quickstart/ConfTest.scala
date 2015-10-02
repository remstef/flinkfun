package org.myorg.quickstart

import com.typesafe.config.{ConfigValue, ConfigFactory}
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._


/**
 * Created by Steffen Remus.
 */
object ConfTest {

  val conf = ConfigFactory.load()
  val LOG = LoggerFactory.getLogger(ConfTest.getClass)

  def main(args: Array[String]) {
    System.setProperty("DT.question","What?")

    for(entry <- conf.getConfig("DT").entrySet()){
      println(entry.getKey + "\t" + entry.getValue.render() + ":" + entry.getValue.valueType())
    }

    LOG.trace("this is a trace message")
    LOG.debug("this is a debug message")
    LOG.info("this is a info message")
    LOG.warn("this is a warn message")
    LOG.error("this is a error message")




  }


}
