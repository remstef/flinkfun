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

package de.tudarmstadt.lt.flinkdt.types

import de.tudarmstadt.lt.flinkdt.Implicits._
import de.tudarmstadt.lt.flinkdt.tasks.DSTaskConfig
import scala.math._

/**
  *  DEFAULT CT2
  *                |  B      !B     | SUM
  *             ---------------------------
  *  CT2(A,B) =  A |  n11    n12    | n1.
  *             !A |  n21    n22    | n2.
  *             ---------------------------
  *                |  n.1    n.2    | n
  *
  *
  * !!!! n.. must be always at least max{ n1. + (n.1 - n11), n.1 + (n1. - n11) }, when setting n11 to 0 -> (n1. + (n.1 - n11)) == (n.1 + (n1. - n11)) !!!!
  *
  * Created by Steffen Remus.
  *
  */
@SerialVersionUID(42L)
case class CT2def[T1, T2](var a:T1, var b:T2,
                          var n11:Double   = 1f,
                          var n1dot:Double = 1f,
                          var ndot1:Double = 1f,
                          var n:Double     = 1f) extends CT2 {

  override type typeA = T1
  override type typeB = T2

  override def n12   = n1dot - n11
  override def n21   = ndot1 - n11
  override def n22   = ndot2 - n12 // n2dot - n21

  override def n2dot = n     - n1dot
  override def ndot2 = n     - ndot1

  def log_pA():Double = (log(n1dot) - log(n))
  def log_pB():Double = (log(ndot1) - log(n))
  def log_pAB():Double = (log(n11) - log(n))
  def log_pAgivenB():Double = (log(n11) - log(n1dot))
  def log_pBgivenA():Double = (log(n11) - log(ndot1))

  /**
    * @return log( p(a,b) / p(a)p(b) )
    */
  def log_pmi():Double = (log(n11) + log(n) - log(n1dot) - log(ndot1))
  def log2_pmi():Double = (log_pmi / log(2))

  // correlation coefficient
  // ((n11*n22)-(n12*n21)) / Math.sqrt(n1dot*n2dot*ndot1*ndot2) toDouble;
  def corrCoeff():Double = (log(n11) + log(n12)) - (log(n12) + log(n21)) / Math.sqrt(log(n1dot) + log(n2dot) + log(ndot1) + log(ndot2)) toDouble


  /**
    * @return
    */
  def lmi():Double = n11 * log2_pmi

  def +(other:CT2def[T1, T2]):this.type = {
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
  def +=(other:CT2def[T1, T2]):this.type = synchronized {
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

  override def prettyprint():String = {

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
    s"""+++ ${getClass.getSimpleName}    ${if(DSTaskConfig.flipct) "[FLIPPED]"} +++
  A = ${a.asString}     B = ${b.asString}
                |  B ${filler}        !B  ${filler}      | SUM
             ---------------------------------${filler_}
  CT2(A,B) =  A |  n11 = ${vf(0)}    n12 = ${vf(1)}    | n1. = ${vf(4)}
             !A |  n21 = ${vf(2)}    n22 = ${vf(3)}    | n2. = ${vf(5)}
             ---------------------------------${filler_}
                |  n.1 = ${vf(6)}    n.2 = ${vf(7)}    | n = ${vf(8)}

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

  def toStringTuple():(String, String, String, String, String, String) = (
    s"${if (DSTaskConfig.flipct) b.asString else a.asString}",
    s"${if (DSTaskConfig.flipct) a.asString else b.asString}",
    s"${n11.asString}",
    s"${(if (DSTaskConfig.flipct) ndot1 else n1dot).asString}",
    s"${(if (DSTaskConfig.flipct) n1dot else ndot1).asString}",
    s"${n.asString}")

  def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3, t._4, t._5, t._6)
  }

  def asCT2red() = CT2red[T1,T2](a,b,n11)

  def asCT2ext(n1dot:Double=n11, ndot1:Double=n11, n:Double=n11, o1dot:Double=1, odot1:Double=1, on:Double=1):CT2ext[T1,T2] = CT2ext(a,b,n11,n1dot,ndot1,n,o1dot,odot1,on)

  override def flipped() : CT2def[T2, T1] = copy(a = b, b = a, n1dot = ndot1, ndot1 = n1dot)

}
