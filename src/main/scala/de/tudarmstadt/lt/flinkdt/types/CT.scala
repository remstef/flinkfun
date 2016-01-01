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

package de.tudarmstadt.lt.flinkdt.types

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
abstract class CT[T1, T2] extends Serializable with Comparable[CT[T1,T2]] with Cloneable{

  def a:T1
  def b:T2

  override def compareTo(o: CT[T1,T2]): Int = {
//    val c = a.compareTo(o.a)
//    if(c != 0)
//      return b.compareTo(o.b)
//    return c
    ???
  }

  def copyClone():this.type = clone().asInstanceOf[this.type]

  def copyDeep():this.type = {
    val serialize = new ByteArrayOutputStream()
    new ObjectOutputStream(serialize).writeObject(this)
    val deserialize = new ByteArrayInputStream(serialize.toByteArray());
    return new ObjectInputStream(deserialize).readObject().asInstanceOf[this.type];
  }

}
