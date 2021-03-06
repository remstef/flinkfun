/*
 *  Copyright (c) 2016
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

package de.tudarmstadt.lt.flinkdt.textutils

import de.tudarmstadt.lt.scalautils.FormatUtils
import de.tudarmstadt.lt.utilities.HashUtils

import scala.reflect._

/**
  * Created by Steffen Remus.
  */
object StringConvert {

  def convert_toType[T : ClassTag](x:String) = classTag[T] match {
    case t if t == classTag[String] => x.asInstanceOf[T]
    case t if t == classTag[Array[Byte]] => HashUtils.decodeHexString(x).asInstanceOf[T]
    case t if t == classTag[Int] => x.toInt.asInstanceOf[T]
    case t if t == classTag[Long] => x.toLong.asInstanceOf[T]
    case t if t == classTag[Double] => x.toDouble.asInstanceOf[T]
    case t if t == classTag[Float] => x.toFloat.asInstanceOf[T]
    case t if t == classTag[Short] => x.toShort.asInstanceOf[T]
    case t if t == classTag[Byte] => x.toByte.asInstanceOf[T]
  }

  def convert_toString(x:Any) =  x match {
    case t:Double => FormatUtils.format(x.asInstanceOf[Number])
    case t:Float => FormatUtils.format(x.asInstanceOf[Number])
    case t:Number => t.toString()
    case t:Array[Byte] => HashUtils.encodeHexString(x.asInstanceOf[Array[Byte]])
    case _ => x.toString()
  }

}
