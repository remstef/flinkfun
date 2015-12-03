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

import scala.math._

/**
  * Created by Steffen Remus.
  */
object CT2 {

  val EMPTY_CT = new CT2[String, String](A = "", B = "", n11 = 0f, n1dot = 0f, ndot1 = 0f, n = 0f)

  def fromString(ct2AsString:String):CT2[String,String] = fromStringArray(ct2AsString.split("\t"))

  def fromStringArray(ct2AsStringArray:Array[String]):CT2[String,String] = {
    ct2AsStringArray match {
      case  Array(_A,_B,n11,n1dot,ndot1,n,ndocs) => new CT2(_A,_B,n11.toFloat,n1dot.toFloat,ndot1.toFloat,n.toFloat)
      case  Array(_A,_B,n11,n1dot,ndot1,n)       => new CT2(_A,_B,n11.toFloat,n1dot.toFloat,ndot1.toFloat,n.toFloat)
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
 *
 */
@SerialVersionUID(42L)
case class CT2[T1,T2](var A:T1, var B:T2,
                  var n11:Float   = 1f,
                  var n1dot:Float = 1f,
                  var ndot1:Float = 1f,
                  var n:Float     = 1f,
                  val srcid:Option[Any] = None,
                  val isflipped:Boolean = false) extends Serializable with Cloneable {

  def n12()   = n1dot - n11
  def n21()   = ndot1 - n11
  def n2dot() = n     - n1dot
  def ndot2() = n     - ndot1
  def n22()   = ndot2 - n12 // n2dot - n21

  def pmi():Float = ((log(n11) + log(n)) - (log(n1dot) + log(ndot1))).toFloat
  def lmi():Float = (n11 * pmi()).toFloat

  def +(other:CT2[T1, T2]):this.type = {
    val newct:this.type = copy().asInstanceOf[this.type]
    newct.n += other.n11
    if(A == other.A) {
      if(B == other.B){
        newct.n11   += other.n11
        newct.n1dot += other.n11
        newct.ndot1 += other.n11
        return newct
      }
      newct.n1dot += other.n11
      return newct
    }
    if(this.B == other.B)
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
    if(this.A == other.A) {
      if(this.B == other.B){
        this.n11   += other.n11
        this.n1dot += other.n11
        this.ndot1 += other.n11
        return this
      }
      this.n1dot += other.n11
      return this
    }
    if(B == other.B)
      this.ndot1 += other.n11
    return this
  }

  def prettyPrint():String = {
    val v = Array(
      s"${FormatUtils.format(n11)}",
      s"${FormatUtils.format(n12)}",
      s"${FormatUtils.format(n21)}",
      s"${FormatUtils.format(n22)}",
      s"${FormatUtils.format(n1dot)}",
      s"${FormatUtils.format(n2dot)}",
      s"${FormatUtils.format(ndot1)}",
      s"${FormatUtils.format(ndot2)}",
      s"${FormatUtils.format(n)}")
    val maxwidth = v.map(_.length).max + 2
    val vf = v.map(x => ("%-"+maxwidth+"s").format(x)).toIndexedSeq
    val filler  = " "*maxwidth
    val filler_ = "-"*2*maxwidth
    val source = if(srcid.isDefined) s"source = ${srcid.get}" else ""
    s"""+++ ${getClass.getSimpleName}    ${source}
  A = ${A}     B = ${B}
                |  B ${filler}        !B  ${filler}      | SUM
             ---------------------------------${filler_}
  CT2(A,B) =  A |  n11 = ${vf(0)}    n12 = ${vf(1)}    | n1dot = ${vf(4)}
             !A |  n21 = ${vf(2)}    n22 = ${vf(3)}    | n2dot = ${vf(5)}
             ---------------------------------${filler_}
                |  ndot1 = ${vf(6)}  ndot2 = ${vf(7)}  | n = ${vf(8)}
"""
  }

  def toStringTuple():(String, String, String, String, String, String) = (
    s"${A}",
    s"${B}",
    s"${FormatUtils.format(n11)}",
    s"${FormatUtils.format(n1dot)}",
    s"${FormatUtils.format(ndot1)}",
    s"${FormatUtils.format(n)}")

  def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3, t._4, t._5, t._6)
  }

  override def toString():String = toStringArray().mkString("\t")

  override def equals(that:Any):Boolean = basicEquals(that)

  override def hashCode():Int = basicHashCode()

  def basicEquals(that:Any):Boolean = {
    if(that.isInstanceOf[this.type ]) {
      val ct2 = that.asInstanceOf[this.type]
      return  ((this.A == ct2.A) && (this.B == ct2.B))
    }
    return false
  }

  def basicHashCode():Int = {
    41 * (
      41 + (if(null == A) 0 else  A.hashCode)
      ) + (if(null == B) 0 else  B.hashCode)
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
      this.B,
      this.A,
      this.n11,
      this.ndot1,
      this.n1dot,
      this.n,
      this.srcid,
      true
    )
  }

}
