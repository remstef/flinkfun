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

import scala.reflect._

/**
  * Created by Steffen Remus.
  */
object StringConvert {

  def convert(x: String) = new {
    def toT[T : ClassTag]:T = classTag[T] match {
      case t if t == classTag[Int] => x.toInt.asInstanceOf[T]
      case t if t == classTag[String] => x.asInstanceOf[T]
    }
  }



}
