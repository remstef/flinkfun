package de.tudarmstadt.lt.flinkdt.types

import de.tudarmstadt.lt.flinkdt.Implicits._
import de.tudarmstadt.lt.flinkdt.tasks.DSTaskConfig

/**
  * REDUCED CT2
  *
  *                |  B      !B     | SUM
  *             ---------------------------
  *  CT2(A,B) =  A |  n11    0      | n11
  *             !A |  0      0      | 0
  *             ---------------------------
  *                |  n11    0      | n11
  *
  * Created by Steffen Remus
  *
  */

@SerialVersionUID(42L)
case class CT2red[T1, T2](var a:T1,
                          var b:T2,
                          var n11:Float = 1f) extends CT2 {

  override type typeA = T1
  override type typeB = T2

  override def n1dot: Float = n11
  override def ndot1: Float = n11
  override def n: Float = n11

  override def n12: Float = 0f
  override def n21: Float = 0f
  override def n22: Float = 0f

  override def n2dot: Float = 0f
  override def ndot2: Float = 0f


  def +(other:CT2red[T1, T2]):this.type = {
    val newct:this.type = copy().asInstanceOf[this.type]
    if(a == other.a && b == other.b)
      newct.n11 += other.n11
    return newct
  }

  /**
    * mutually add and change only this CT2
    *
    * @param other
    * @return
    */
  def +=(other:CT2red[T1, T2]):this.type = synchronized {
    if(a == other.a && b == other.b)
      n11 += other.n11
    return this
  }

  def asCT2def(n1dot:Float=n11, ndot1:Float=n11, n:Float=n11):CT2def[T1,T2] = CT2def(a,b,n11,n1dot,ndot1,n)

  def asCT2ext(n1dot:Float=n11, ndot1:Float=n11, n:Float=n11, o1dot:Float=1f, odot1:Float=1f, on:Float=1f):CT2ext[T1,T2] = CT2ext(a,b,n11,n1dot,ndot1,n,o1dot,odot1,on)

  def toStringTuple():(String, String, String) = (
    s"${if (DSTaskConfig.flipct) b.asString else a.asString}",
    s"${if (DSTaskConfig.flipct) a.asString else b.asString}",
    s"${n11.asString}")

  override def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3)
  }

  override def equals(that:Any):Boolean = {
    if(that.isInstanceOf[this.type ]) {
      val ct2 = that.asInstanceOf[this.type]
      return  ((this.a == ct2.a) && (this.b == ct2.b))
    }
    return false
  }

  override def hashCode():Int = {
    41 * (
      41 + (if(null == a) 0 else  a.hashCode)
      ) + (if(null == b) 0 else  b.hashCode)
  }

  override def flipped() : CT2red[T2, T1] = copy(a = b, b = a)


}
