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

import de.tudarmstadt.lt.flinkdt.Implicits._
import de.tudarmstadt.lt.flinkdt.tasks.DSTaskConfig

import scala.math._


/**
  *  EXTENDED CT2
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
  * !!!! n.. must be always at least max{ n1. + (n.1 - n11), n.1 + (n1. - n11) }, when setting n11 to 0 -> (n1. + (n.1 - n11)) == (n.1 + (n1. - n11)) !!!!
  *
  * Created by Steffen Remus.
  *
  */
@SerialVersionUID(42L)
case class CT2ext[T1, T2](var a:T1, var b:T2,
                          var n11:Double   = 1f,
                          var n1dot:Double = 1f,
                          var ndot1:Double = 1f,
                          var n:Double     = 1f,
                          var o1dot:Double = 1f,
                          var odot1:Double = 1f,
                          var on:Double    = 1f) extends CT2 {

  override type typeA = T1
  override type typeB = T2

  override def n12   = n1dot - n11
  override def n21   = ndot1 - n11
  override def n22   = ndot2 - n12 // n2dot - n21

  override def n2dot = n     - n1dot
  override def ndot2 = n     - ndot1

  def o11   = 1f
  def o12   = o1dot - o11
  def o21   = odot1 - o11
  def o22   = odot2 - o12 // o2dot - o21

  def o2dot = on    - o1dot
  def odot2 = on    - odot1

  def log_pA_n():Double = (log(n1dot) - log(n))
  def log_pB_n():Double = (log(ndot1) - log(n))
  def log_pAB_n():Double = (log(n11) - log(n))
  def log_pAgivenB_n():Double = (log(n11) - log(n1dot))
  def log_pBgivenA_n():Double = (log(n11) - log(ndot1))

  /**
    * @return log( p(a,b) / p(a)p(b) )
    */
  def log_pmi_n():Double = (log(n11) + log(n) - log(n1dot) - log(ndot1))
  def log2_pmi_n():Double = (log_pmi_n / log(2))
  def lmi_n():Double = n11 * log2_pmi_n

  def log_pA_o():Double = (log(n1dot) - log(on))
  def log_pB_o():Double = (log(ndot1) - log(on))
  def log_pAB_o():Double = -log(n)
  def log_pAgivenB_o():Double = -log(o1dot)
  def log_pBgivenA_o():Double = -log(odot1)

  // log( (1/n) / p(a)p(b) )
  def log_pmi_o():Double = (log(n) - log(n1dot) - log(ndot1))
  def log2_pmi_o():Double = (log_pmi_o / log(2))
  def lmi_o():Double = log2_pmi_o // 1 * pmi


  def +(other:CT2ext[T1, T2]):this.type = {
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
      newct.o1dot += 1f
    }
    if(this.b == other.b) {
      newct.ndot1 += other.n11
      newct.odot1 += 1f
    }
    newct.on += 1f
    return newct
  }

  /**
    * mutually add and change only this CT2
    *
    * @param other
    * @return
    */
  def +=(other:CT2ext[T1, T2]):this.type = synchronized {
    this.n += other.n11
    if(this.a == other.a) {
      if(this.b == other.b){
        this.n11   += other.n11
        this.n1dot += other.n11
        this.ndot1 += other.n11
        return this
      }
      this.n1dot += other.n11
      this.odot1 += 1f
    }
    if(b == other.b) {
      this.ndot1 += other.n11
      this.odot1 += 1f
    }
    this.on += 1f
    return this
  }

  override def requireConsistency(fail_quietly:Boolean = true):Boolean = {
    if(!super.requireConsistency(fail_quietly))
      return false
    try {
      require(1 <= o1dot, "Check '1 <= o1.' failed \n" + prettyprint)
      require(1 <= odot1, "Check '1 <= o.1' failed \n" + prettyprint)
      require(1 <= on, "Check '1 <= on' failed \n" + prettyprint)
      require(o1dot <= on, "Check 'o1. <= on' failed \n" + prettyprint)
      require(odot1 <= on, "Check 'o.1 <= on' failed \n" + prettyprint)
      require(on >= o1dot + odot1 - 1, "Check 'on >= o1. + o.1 - 1' failed \n" + prettyprint)
      require(on >= odot1 + o1dot - 1, "Check 'on >= o.1 + o1. - 1' failed \n" + prettyprint)
      require(o12 >= 0, "Check 'o12 >= 0' failed \n" + prettyprint)
      require(o21 >= 0, "Check 'o21 >= 0' failed \n" + prettyprint)
      require(o22 >= 0, "Check 'o22 >= 0' failed \n" + prettyprint)
      require(o11 <= n11, "Check 'o11 <= n11' failed \n" + prettyprint)
      require(o1dot <= n1dot, "Check 'o1. <= n1.' failed \n" + prettyprint)
      require(odot1 <= ndot1, "Check 'o.1 <= n.1' failed \n" + prettyprint)
      require(on <= n, "Check 'on <= n' failed \n" + prettyprint)
    }catch{
      case e : Throwable if fail_quietly => return false
      case e : Throwable => throw e
    }
    return true
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
      n.asString,

      1f.asString,
      o12.asString,
      o21.asString,
      o22.asString,
      o1dot.asString,
      o2dot.asString,
      odot1.asString,
      odot2.asString,
      on.asString
    )

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

  log p(A,B)    = ${log_pAB_n.asString}
  log p(A)      = ${log_pA_n.asString}
  log p(B)      = ${log_pB_n.asString}
  log p(A|B)    = ${log_pAgivenB_n.asString}
  log p(B|A)    = ${log_pBgivenA_n.asString}
  log pmi(A,B)  = ${log_pmi_n.asString}
  log2 pmi(A,B) = ${log2_pmi_n.asString}
  lmi(A,B)      = ${lmi_n.asString}


                 |  B ${filler}        !B  ${filler}      | SUM
  Occurrences ---------------------------------${filler_}
  CT2(A,B) =   A |  o11 = ${vf(9)}    o12 = ${vf(10)}    | o1. = ${vf(13)}
              !A |  o21 = ${vf(11)}    o22 = ${vf(12)}    | o2. = ${vf(14)}
              ---------------------------------${filler_}
                 |  o.1 = ${vf(15)}    o.2 = ${vf(16)}    | on = ${vf(17)}

  log p(A,B)    = ${log_pAB_o.asString}
  log p(A)      = ${log_pA_o.asString}
  log p(B)      = ${log_pB_o.asString}
  log p(A|B)    = ${log_pAgivenB_o.asString}
  log p(B|A)    = ${log_pBgivenA_o.asString}
  log pmi(A,B)  = ${log_pmi_o.asString}
  log2 pmi(A,B) = ${log2_pmi_o.asString}
  lmi(A,B)      = ${lmi_o.asString}

"""
  }

  def toStringTuple():(String, String, String, String, String, String, String, String, String) = (
    s"${if (DSTaskConfig.flipct) b.asString else a.asString}",
    s"${if (DSTaskConfig.flipct) a.asString else b.asString}",
    s"${n11.asString}",
    s"${(if (DSTaskConfig.flipct) ndot1 else n1dot).asString}",
    s"${(if (DSTaskConfig.flipct) n1dot else ndot1).asString}",
    s"${n.asString}",
    s"${(if (DSTaskConfig.flipct) odot1 else o1dot).asString}",
    s"${(if (DSTaskConfig.flipct) o1dot else odot1).asString}",
    s"${on.asString}"
    )

  def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9)
  }

  def toCT2Min() = CT2red[T1,T2](a,b,n11)
  def toCT2Full() = CT2def[T1,T2](a,b,n11,n1dot,ndot1,n)

  override def flipped() : CT2ext[T2, T1] = copy(a = b, b = a, n1dot = ndot1, ndot1 = n1dot, o1dot = odot1, odot1 = o1dot)

}
