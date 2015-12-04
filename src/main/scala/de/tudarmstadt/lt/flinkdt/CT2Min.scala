package de.tudarmstadt.lt.flinkdt

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import de.tudarmstadt.lt.scalautils.FormatUtils

/**
  * Created by Steffen Remus
  */
object CT2Min {


  def EMPTY_CT[T1,T2] = new CT2Min[T1, T2](A = null.asInstanceOf[T1], B = null.asInstanceOf[T2], n11 = 0f)

  def fromString[T1,T2](ct2AsString:String):CT2Min[T1,T2] = fromStringArray(ct2AsString.split("\t"))

  def fromStringArray[T1,T2](ct2AsStringArray:Array[String]):CT2Min[T1,T2] = {
    ct2AsStringArray match {
      case  Array(_A,_B,n11,_*) => new CT2Min[T1,T2](_A.asInstanceOf[T1], _B.asInstanceOf[T2], n11.toFloat)
      case  Array(_A,_B)     => new CT2Min(_A.asInstanceOf[T1], _B.asInstanceOf[T2], 1f)
      case _ => EMPTY_CT:CT2Min[T1,T2]
    }
  }

}



@SerialVersionUID(42L)
case class CT2Min[T1,T2](var A:T1,
                    var B:T2,
                    var n11:Float = 1f) extends Serializable with Cloneable {

  def +(other:CT2Min[T1, T2]):this.type = {
    val newct:this.type = copy().asInstanceOf[this.type]
    if(A == other.A && B == other.B)
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
    if(A == other.A && B == other.B)
      n11 += other.n11
    return this
  }

  def toCT2():CT2[T1,T2] = CT2(A,B,n11)

  def prettyPrint():String = {
    val v = s"${FormatUtils.format(n11)}"
    val width = v.length + 2
    val vf = ("%-"+width+"s").format(v)
    val filler  = " "*width
    val filler_ = "-"*width

    s"""+++ ${getClass.getSimpleName}
  A = ${A}     B = ${B}
        |  B       ${filler}   !B        | SUM
             ---------------------------------${filler_}
  CT2(A,B) =  A |  n11 = ${v}    n12 = ?    | n1dot = ?
             !A |  n21 = ?${filler}   n22 = ?    | n2dot = ?
             ----------------------------------------${filler_}
        |  ndot1 = ?${filler} ndot2 = ?  | n = ?
"""
  }

  def toStringTuple():(String, String, String) = (
    s"${A}",
    s"${B}",
    s"${FormatUtils.format(n11)}")

  def toStringArray():Array[String] = {
    val t = toStringTuple()
    Array(t._1, t._2, t._3)
  }

  override def toString():String = toStringArray().mkString("\t")

  override def equals(that:Any):Boolean = {
    if(that.isInstanceOf[this.type ]) {
      val ct2 = that.asInstanceOf[this.type]
      return  ((this.A == ct2.A) && (this.B == ct2.B))
    }
    return false
  }

  override def hashCode():Int = {
    41 * (
      41 + (if(null == A) 0 else  A.hashCode)
      ) + (if(null == B) 0 else  B.hashCode)
  }


}
