package de.tudarmstadt.lt.flinkdt.types

import de.tudarmstadt.lt.flinkdt.StringConvert._

/**
  * Created by Steffen Remus
  */
/*
 *                |  B      !B     | SUM
 *             ---------------------------
 *  CT2(A,B) =  A |  n11    0      | n11
 *             !A |  0      0      | 0
 *             ---------------------------
 *                |  n11    0      | n11
 */

@SerialVersionUID(42L)
case class CT2Min[T1, T2](var a:T1,
                          var b:T2,
                          var n11:Float = 1f) extends CT2[T1,T2] {

  override def n1dot: Float = n11
  override def ndot1: Float = n11
  override def n: Float = n11

  override def n12: Float = 0f
  override def n21: Float = 0f
  override def n22: Float = 0f

  override def n2dot: Float = 0f
  override def ndot2: Float = 0f


  def +(other:CT2Min[T1, T2]):this.type = {
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
  def +=(other:CT2Min[T1, T2]):this.type = synchronized {
    if(a == other.a && b == other.b)
      n11 += other.n11
    return this
  }

  def asCT2Full(n1dot:Float=n11, ndot1:Float=n11, n:Float=n11):CT2Full[T1,T2] = CT2Full(a,b,n11,n1dot,ndot1,n)

  def toStringTuple():(String, String, String) = (
    s"${a.asString}",
    s"${b.asString}",
    s"${n11.asString}")

  def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3)
  }

  override def toString():String = toStringArray().mkString("\t")

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


}
