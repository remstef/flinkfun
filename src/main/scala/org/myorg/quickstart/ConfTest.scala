package org.myorg.quickstart

import com.typesafe.config.{ConfigValue, ConfigFactory}
import scala.collection.JavaConversions._


/**
 * Created by Steffen Remus.
 */
object ConfTest {

  def main(args: Array[String]) {
    val conf = ConfigFactory.load()

    for(entry <- conf.getConfig("DT").entrySet()){
      println(entry.getKey + "\t" + entry.getValue.render() + ":" + entry.getValue.valueType())
    }
    
  }


}
