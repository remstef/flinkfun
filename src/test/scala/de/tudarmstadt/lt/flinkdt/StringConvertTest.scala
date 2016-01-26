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

import org.apache.flink.types.StringValue

/**
  * Created by Steffen Remus.
  */
object StringConvertTest {
  def main(args: Array[String]) {
    new StringConvertTest().mainn(args);
  }
}
class StringConvertTest {

  val a:Int = 1010000000


  implicit def string_conversion(x: Any) = new {
    def asString():String = conv(x)
  }

  def conv(x:Any) = x match {
    case t:CharSequence => "buhuhu"
    case t:Number => "bo"
    case t:Array[Byte] => "yay"

    case _ => x.toString
  }

  def mainn(args: Array[String]) {
    println(a.asString())
    val b = "asda"
    println(b.asString())
    val c:Int = 1010000000
    println(c.asString())
    val d:Array[Byte] = Array(1.toByte,2.toByte,3.toByte)
    val e:StringValue = new StringValue("hellö")

    println(conv(a))
    println(conv(b))
    println(conv(c))
    println(conv(d))
    println(conv(e))


    println(a.getClass)
    println(b.getClass)
    println(c.getClass)
  }

}
