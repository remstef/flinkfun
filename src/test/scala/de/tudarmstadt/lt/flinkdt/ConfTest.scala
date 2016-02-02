/*
 *  Copyright (c) 2015
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.impl.{SimpleConfigObject, SimpleConfig}
import com.typesafe.config.{ConfigValueFactory, ConfigRenderOptions, Config, ConfigFactory}
import de.tudarmstadt.lt.flinkdt.tasks.DSTaskConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


/**
 * Created by Steffen Remus.
 */
class ConfTest extends FunSuite {

  val LOG = LoggerFactory.getLogger(classOf[ConfTest])

  test("Test configuration via typesafe:config") {

    var conf:Config = null
    conf = ConfigFactory.parseFile(new File("app.conf")).withFallback(ConfigFactory.load()).resolve()

    System.setProperty("dt.question","What?")

    for(entry <- conf.getConfig("dt").entrySet()){
      println(entry.getKey + "\t" + entry.getValue.render() + ":" + entry.getValue.valueType())
    }

    println(conf.getConfig("dt").getConfig("output").hasPath("dt"))

    LOG.trace("this is a trace message")
    LOG.debug("this is a debug message")
    LOG.info("this is a info message")
    LOG.warn("this is a warn message")
    LOG.error("this is a error message")

  }

  test("write current config") {
    print(DSTaskConfig.toString())
  }

}

//object testapp extends App {
//
//  val p = ParameterTool.fromArgs(args)
//  println(p.getProperties)
//  println(p.get("__NO_VALUE_KEY"))
//  println(p.toMap.keySet())
//
//}
