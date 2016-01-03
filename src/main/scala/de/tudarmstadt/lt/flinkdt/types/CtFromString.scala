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

package de.tudarmstadt.lt.flinkdt.types

import de.tudarmstadt.lt.flinkdt.StringConvert._

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect._

/**
  * Created by Steffen Remus.
  */
object CtFromString {

  def apply[C <: CT[T1,T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](ct2AsString:String):C = fromString[C, T1, T2](ct2AsString)

  def EMPTY_CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] = new CT2[T1, T2](a = null.asInstanceOf[T1], b = null.asInstanceOf[T2], n11 = 0f, n1dot = 0f, ndot1 = 0f, n = 0f)

  def EMPTY_CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] = new CT2Min[T1, T2](a = null.asInstanceOf[T1], b = null.asInstanceOf[T2], n11 = 0f)

  def fromString[C <: CT[T1,T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](ct2AsString:String):C = classTag[C] match {
    case t if t == classTag[CT2[T1,T2]] => _CT2[T1,T2](ct2AsString.split("\t")).asInstanceOf[C]
    case t if t == classTag[CT2Min[T1,T2]] => _CT2Min[T1,T2](ct2AsString.split("\t")).asInstanceOf[C]
    case _ => null.asInstanceOf[C]
  }

  def _CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](ct2AsStringArray:Array[String]):CT2[T1,T2] = {
    ct2AsStringArray match {
      case  Array(_A,_B,n11,n1dot,ndot1,n,_*) => CT2[T1,T2](_A.toT[T1],_B.toT[T2],n11.toFloat,n1dot.toFloat,ndot1.toFloat,n.toFloat)
      case _ => EMPTY_CT2
    }
  }

  def _CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](ct2AsStringArray: Array[String]): CT2Min[T1, T2] = {
    ct2AsStringArray match {
      case Array(_A, _B, n11, _*) => CT2Min[T1, T2](_A.toT[T1], _B.toT[T2], n11.toFloat)
      case Array(_A, _B) => CT2Min(_A.toT[T1], _B.toT[T2], 1f)
      case _ => EMPTY_CT2Min: CT2Min[T1, T2]
    }
  }

}
