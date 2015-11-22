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

import java.text.DecimalFormat

import scala.math._

/**
 * Created by Steffen Remus.
 */
object CT2 {

  val EMPTY_CT = CT2[String](A = "", B = "", n11 = 0f, n1dot = 0f, ndot1 = 0f, n = 0f, ndocs = 0)

  def fromString(ct2AsString:String):CT2[String] = fromStringArray(ct2AsString.split("\t"))

  def fromStringArray(ct2AsStringArray:Array[String]):CT2[String] = {
    ct2AsStringArray match {
      case  Array(_A,_B,n11,n1dot,ndot1,n,ndocs) => CT2(_A,_B,n11.toFloat,n1dot.toFloat,ndot1.toFloat,n.toFloat,ndocs.toInt)
      case  Array(_A,_B,n11,n1dot,ndot1,n)       => CT2(_A,_B,n11.toFloat,n1dot.toFloat,ndot1.toFloat,n.toFloat,1)
      case _ => EMPTY_CT
    }
  }

  val nf = new DecimalFormat("##0.######E00")

  def format(number: Number):String = nf.format(number).replace("E00","")

  def +[T](ct1:CT2[T], ct2:CT2[T], add_ndocs:Boolean):Seq[CT2[T]] = {
    val ndocs1 = if(add_ndocs) ct1.ndocs + ct2.ndocs else ct1.ndocs
    val ndocs2 = if(add_ndocs) ct1.ndocs + ct2.ndocs else ct2.ndocs
    if(ct1.A == ct2.A) {
      if(ct1.B == ct2.B){
        val newct = ct1.copy[T](
          n11   = ct1.n11   + ct2.n11,
          n1dot = ct1.n1dot + ct2.n11,
          ndot1 = ct1.ndot1 + ct2.n11,
          n     = ct1.n     + ct2.n11,
          ndocs = ndocs1
        )
        return Seq(newct) // return only one ct in case of perfect match
      }
      val newct1 = ct1.copy[T](n1dot = ct1.n1dot + ct2.n11, n = ct1.n + ct2.n11, ndocs = ndocs1)
      val newct2 = ct2.copy[T](n1dot = ct2.n1dot + ct1.n11, n = ct2.n + ct1.n11, ndocs = ndocs2)
      return Seq(newct1, newct2)
    }
    if(ct1.B == ct2.B){
      val newct1 = ct1.copy[T](ndot1 = ct1.ndot1 + ct2.n11, n = ct1.n + ct2.n11, ndocs = ndocs1)
      val newct2 = ct2.copy[T](ndot1 = ct2.ndot1 + ct1.n11, n = ct2.n + ct1.n11, ndocs = ndocs2)
      return Seq(newct1, newct2)
    }
    val newct1 = ct1.copy[T](n = ct1.n + ct2.n11, ndocs = ndocs1)
    val newct2 = ct2.copy[T](n = ct2.n + ct1.n11, ndocs = ndocs2)
    return Seq(newct1, newct2)
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

case class CT2[T](A:T, B:T,
                  var n11:Float   = 1f,
                  var n1dot:Float = 1f,
                  var ndot1:Float = 1f,
                  var n:Float     = 1f,
                  var ndocs:Int   = 1) {

  def n12()   = n1dot - n11
  def n21()   = ndot1 - n11
  def n2dot() = n     - n1dot
  def ndot2() = n     - ndot1
  def n22()   = ndot2 - n12 // n2dot - n21

  def pmi() = (log(n11) + log(n)) - (log(n1dot) + log(ndot1))
  def lmi() = n11 * pmi()


  def +(o:CT2[T], add_ndocs:Boolean=false):CT2[T] = CT2.+(this, o, add_ndocs)(0)

  /**
    * mutually add and change this CT2 and! other CT2
    *
    * @param other
    * @return
    */
  def mutableAdd(other:CT2[T], add_ndocs:Boolean):Unit = {
    this.ndocs  = if(add_ndocs) this.ndocs + other.ndocs else this.ndocs
    other.ndocs = if(add_ndocs) this.ndocs + other.ndocs else other.ndocs
    if(this.A == other.A) {
      if(other.B == other.B){
        this.n11    += other.n11
        this.n1dot  += other.n11
        this.ndot1  += other.n11
        this.n      += other.n11
        other.n11   += this.n11
        other.n1dot += this.n11
        other.ndot1 += this.n11
        other.n     += this.n11
        return
      }
      this.n1dot  += other.n11
      this.n      += other.n11
      other.n1dot += this.n11
      other.n     += this.n11
      return
    }
    if(this.B == other.B){
      this.ndot1  += other.n11
      this.n      += other.n11
      other.ndot1 += this.n11
      other.n     += this.n11
      return
    }
    this.n      += other.n11
    other.n     += this.n11
    return
  }

  def toStringTuple():(String, String, String, String, String, String, String) = (
    s"${A}",
    s"${B}",
    s"${CT2.format(n11)}",
    s"${CT2.format(n12)}",
    s"${CT2.format(n21)}",
    s"${CT2.format(n22)}",
    f"${ndocs}%d")

  def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3, t._4, t._5, t._6, t._7)
  }

  override def toString():String = toStringArray().mkString("\t")

}
