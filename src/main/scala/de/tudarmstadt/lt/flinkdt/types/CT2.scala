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
import de.tudarmstadt.lt.flinkdt.StringConvert._

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

/**
  * Created by Steffen Remus.
  */

/*
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
 */
abstract class CT2(implicit val ordering:Ordering[CT2]) extends Serializable with Ordered[CT2] with Cloneable {

  // TODO: think about : https://twitter.github.io/scala_school/advanced-types.html
  //  scala> trait Foo[M[_]] { type t[A] = M[A] }
  //  defined trait Foo
  //
  //  scala> val x: Foo[List]#t[Int] = List(1)
  //  x: List[Int] = List(1)
  //
  // in order to avoid repetitions, e.g. CtFromString[CT2[Srting,String],String,String] => CtFromString[CT2#T1[String]#T2[String]]
  // scala> abstract class Foo{ type t1; def a:t1}
  // scala> class Bar[T1](val a:T1) extends Foo {type t1=T1}
  // scala> val x:Foo = new Bar("hello")

  type typeA
  type typeB

  def a:typeA
  def b:typeB

  def n11:Float
  def n1dot:Float
  def ndot1:Float
  def n:Float

  def n12:Float
  def n21:Float
  def n2dot:Float
  def ndot2:Float
  def n22:Float

  /**
    * Check consistency:
    *
    * n11 <= n1dot
    * n11 <= ndot1
    * n11 <= n
    * n1dot <= n
    * ndot1 <= n
    * n >= n1dot + ndot1 - n11
    * n >= ndot1 + n1dot - n11
    * n12 >= 0
    * n21 >= 0
    * n22 >= 0
    *
    * @return true if all tests passed
    */
  def requireConsistency(fail_quietly:Boolean = true):Boolean = {
    try {
      require(n11 <= n1dot, "Check 'n11 <= n1.' failed \n" + prettyPrint)
      require(n11 <= ndot1, "Check 'n11 <= n.1' failed \n" + prettyPrint)
      require(n11 <= n, "Check 'n11 <= n' failed \n" + prettyPrint)
      require(n1dot <= n, "Check 'n1. <= n' failed \n" + prettyPrint)
      require(ndot1 <= n, "Check 'n.1 <= n' failed \n" + prettyPrint)
      require(n >= n1dot + ndot1 - n11, "Check 'n >= n1. + n.1 - n11' failed \n" + prettyPrint)
      require(n >= ndot1 + n1dot - n11, "Check 'n >= n.1 + n1. - n11' failed \n" + prettyPrint)
      require(n12 >= 0, "Check 'n12 >= 0' failed \n" + prettyPrint)
      require(n21 >= 0, "Check 'n21 >= 0' failed \n" + prettyPrint)
      require(n22 >= 0, "Check 'n22 >= 0' failed \n" + prettyPrint)
    }catch{
      case e : Throwable if fail_quietly => return false
      case e : Throwable => throw e
    }
    return true
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
    s"""+++ ${getClass.getSimpleName} +++
  A = ${a.asString}     B = ${b.asString}
                |  B ${filler}        !B  ${filler}      | SUM
             ---------------------------------${filler_}
  CT2(A,B) =  A |  n11 = ${vf(0)}    n12 = ${vf(1)}    | n1. = ${vf(4)}
             !A |  n21 = ${vf(2)}    n22 = ${vf(3)}    | n2. = ${vf(5)}
             ---------------------------------${filler_}
                |  n.1 = ${vf(6)}    n.2 = ${vf(7)}    | n = ${vf(8)}

"""
  }

  override def compare(that: CT2): Int = ordering.compare(this,that)

  def copyClone():this.type = clone().asInstanceOf[this.type]

  def copyDeep():this.type = {
    val serialize = new ByteArrayOutputStream()
    new ObjectOutputStream(serialize).writeObject(this)
    val deserialize = new ByteArrayInputStream(serialize.toByteArray())
    return new ObjectInputStream(deserialize).readObject().asInstanceOf[this.type]
  }

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

  def toStringArray():Array[String]

  override def toString():String = toStringArray().mkString("\t")

}
