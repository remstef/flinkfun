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

import de.tudarmstadt.lt.scalautils.FormatUtils
import de.tudarmstadt.lt.utilities.HashUtils

import scala.reflect._

/**
  * Created by Steffen Remus.
  */
object StringConvert {

  implicit def string_conversion(x: String) = new {
    def toT[T : ClassTag]:T = convert_toType(x)
  }

  implicit def string_conversion(x: Any) = new {
    def asString:String = convert_toString(x)
  }

  def convert_toType[T : ClassTag](x:String) = classTag[T] match {
    case t if t == classTag[String] => x.asInstanceOf[T]
    case t if t == classTag[Array[Byte]] => HashUtils.decodeHexString(x).asInstanceOf[T]
    case t if t == classTag[Int] => x.toInt.asInstanceOf[T]
  }

  def convert_toString(x:Any) =  x match {
    case _ if x.isInstanceOf[Number] => FormatUtils.format(x.asInstanceOf[Number])
    case _ if x.isInstanceOf[Array[Byte]] => HashUtils.encodeHexString(x.asInstanceOf[Array[Byte]])
    case _ => x.toString()
  }

}
