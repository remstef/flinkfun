/*
 *
 *  Copyright 2015.
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
 *
 */

package de.tudarmstadt.lt.flinkdt

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import de.tudarmstadt.lt.scalautils.FormatUtils
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.math._
import scala.reflect._

/**
  * Created by Steffen Remus.
  */
object CT2 {

  implicit def string_conversion(x: String) = StringConvert.convert_toType_implicit(x)

  def EMPTY_CT[T1 : ClassTag : TypeInformation, T2: ClassTag : TypeInformation] = new CT2[T1, T2](a = null.asInstanceOf[T1], b = null.asInstanceOf[T2], n11 = 0f, n1dot = 0f, ndot1 = 0f, n = 0f)

  def fromString[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](ct2AsString:String):CT2[T1,T2] = fromStringArray(ct2AsString.split("\t"))

  def fromStringArray[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](ct2AsStringArray:Array[String]):CT2[T1,T2] = {
    ct2AsStringArray match {
      case  Array(_A,_B,n11,n1dot,ndot1,n,_*) => new CT2[T1,T2](_A.toT[T1],_B.toT[T2],n11.toFloat,n1dot.toFloat,ndot1.toFloat,n.toFloat)
      case _ => EMPTY_CT
    }
  }

}

/*
 *                |  B      !B     | SUM
 *             ---------------------------
 *  CT2(A,B) =  A |  n11    n12    | n1dot
 *             !A |  n21    n22    | n2dot
 *             ---------------------------
 *                |  ndot1  ndot2  |  n
 *
 *
 *                |  B      !B     | SUM
 *             ---------------------------
 *  CT2(A,B) =  A |  o11=1  o12    | o1dot
 *             !A |  o21    o22    | o2dot
 *             ---------------------------
 *                |  odot1  odot2  |  on
 *
 */
@SerialVersionUID(42L)
case class CT2[T1, T2](var a:T1, var b:T2,
                       var n11:Float   = 1f,
                       var n1dot:Float = 1f,
                       var ndot1:Float = 1f,
                       var n:Float     = 1f,
                       val srcid:Option[Any] = None,
                       val isflipped:Boolean = false) extends Serializable with Cloneable {

  implicit def string_conversion(x: Any) = StringConvert.convert_toString_implicit(x)

  def n12()   = n1dot - n11
  def n21()   = ndot1 - n11
  def n2dot() = n     - n1dot
  def ndot2() = n     - ndot1
  def n22()   = ndot2 - n12 // n2dot - n21

  def log_pA():Float = (log(n11) - log(n1dot)).toFloat
  def log_pB():Float = (log(n11) - log(ndot1)).toFloat
  def log_pAB():Float = (log(n11) - log(n)).toFloat
  def log_pAgivenB():Float = (log(n11) - log(n1dot)).toFloat
  def log_pBgivenA():Float = (log(n11) - log(ndot1)).toFloat

  /**
    * @return log( p(a,b) / p(a)p(b) )
    */
  def log_pmi():Float = ((log(n11) + log(n)) - (log(n1dot) + log(ndot1))).toFloat
  def log2_pmi():Float = (((log(n11) + log(n)) - (log(n1dot) + log(ndot1))) / log(2)).toFloat

  /**
    * @return
    */
  def lmi():Float = n11 * log2_pmi()

  def +(other:CT2[T1, T2]):this.type = {
    val newct:this.type = copy().asInstanceOf[this.type]
    newct.n += other.n11
    if(a == other.a) {
      if(b == other.b){
        newct.n11   += other.n11
        newct.n1dot += other.n11
        newct.ndot1 += other.n11
        return newct
      }
      newct.n1dot += other.n11
      return newct
    }
    if(this.b == other.b)
      newct.ndot1 += other.n11
    return newct
  }

  /**
    * mutually add and change only this CT2
    *
    * @param other
    * @return
    */
  def +=(other:CT2[T1, T2]):this.type = synchronized {
    this.n += other.n11
    if(this.a == other.a) {
      if(this.b == other.b){
        this.n11   += other.n11
        this.n1dot += other.n11
        this.ndot1 += other.n11
        return this
      }
      this.n1dot += other.n11
      return this
    }
    if(b == other.b)
      this.ndot1 += other.n11
    return this
  }

  def prettyPrint():String = {

    val v = Array(
      n11.asString,
      n12.asString,
      n21.asString,
      n22.asString,
      n1dot.asString,
      n2dot.asString,
      ndot1.asString,
      ndot2.asString,
      n.asString)

    val maxwidth = v.map(_.length).max + 2
    val vf = v.map(x => ("%-"+maxwidth+"s").format(x)).toIndexedSeq
    val filler  = " "*maxwidth
    val filler_ = "-"*2*maxwidth
    val source = if(srcid.isDefined) s"source = ${srcid.get}" else ""
    s"""+++ ${getClass.getSimpleName}    ${source.asString}   +++
  A = ${a.asString}     B = ${b.asString}
                |  B ${filler}        !B  ${filler}      | SUM
             ---------------------------------${filler_}
  CT2(A,B) =  A |  n11 = ${vf(0)}    n12 = ${vf(1)}    | n1dot = ${vf(4)}
             !A |  n21 = ${vf(2)}    n22 = ${vf(3)}    | n2dot = ${vf(5)}
             ---------------------------------${filler_}
                |  ndot1 = ${vf(6)}  ndot2 = ${vf(7)}  | n = ${vf(8)}

  log p(A,B)    = ${log_pAB.asString}
  log p(A)      = ${log_pA.asString}
  log p(B)      = ${log_pB.asString}
  log p(A|B)    = ${log_pAgivenB.asString}
  log p(B|A)    = ${log_pBgivenA.asString}
  log pmi(A,B)  = ${log_pmi.asString}
  log2 pmi(A,B) = ${log2_pmi.asString}
  lmi(A,B)      = ${lmi.asString}

"""
  }

  def toStringTuple():(String, String, String, String, String, String, String, String, String) = (
    s"${a.asString}",
    s"${b.asString}",
    s"${n11.asString}",
    s"${n1dot.asString}",
    s"${ndot1.asString}",
    s"${n.asString}",
    s"${log2_pmi asString}",
    s"${lmi asString}",
    s"${log_pAgivenB asString}")

  def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9)
  }

  def toCT2Min() = CT2Min[T1,T2](a,b,n11)

  override def toString():String = toStringArray().mkString("\t")

  override def equals(that:Any):Boolean = basicEquals(that)

  override def hashCode():Int = basicHashCode()

  def basicEquals(that:Any):Boolean = {
    if(that.isInstanceOf[this.type ]) {
      val ct2 = that.asInstanceOf[this.type]
      return  ((this.a == ct2.a) && (this.b == ct2.b))
    }
    return false
  }

  def basicHashCode():Int = {
    41 * (
      41 + (if(null == a) 0 else  a.hashCode)
      ) + (if(null == b) 0 else  b.hashCode)
  }

  def copyClone():this.type = clone().asInstanceOf[this.type]

  def copyDeep():this.type = {
    val serialize = new ByteArrayOutputStream()
    new ObjectOutputStream(serialize).writeObject(this)
    val deserialize = new ByteArrayInputStream(serialize.toByteArray());
    return new ObjectInputStream(deserialize).readObject().asInstanceOf[this.type];
  }

  def flipped():CT2[T2,T1] = {
    copy(
      this.b,
      this.a,
      this.n11,
      this.ndot1,
      this.n1dot,
      this.n,
      this.srcid,
      true
    )
  }

}
