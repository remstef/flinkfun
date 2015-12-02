package de.tudarmstadt.lt.flinkdt

import java.io.File

import com.typesafe.config.{ConfigFactory, Config}
import de.tudarmstadt.lt.flinkdt.tasks.Executor._

import scala.tools.nsc.Settings

trait T[I,O] extends (I => O) {

  def ~>[X] (g:T[O,X]) =
    C(this, g)

}

object C {

  def apply[I,O,X](f:T[I,X], g:T[X,O]) = new C(f,g)

  class C[I,O,X](f:T[I,X], g:T[X,O]) extends T[I,O] {
    override def apply(s: I): O = g(f(s))
  }

}

object T1 {

  def apply() = new T1()

  class T1 extends T[String, Seq[Char]] {

    override def apply(s: String): Seq[Char] = s.toCharArray

  }

}

object T2 {

  def apply() = new T2()

  class T2 extends T[Seq[Char], Double]{

    override def apply(s:Seq[Char]):Double = s.sum

  }

}

object T3 {

  def apply() = new T3()

  class T3 extends T[Double, String] {

    override def apply(s: Double): String = f"sum = ${s}%f"

  }

}

object T4 {

  def apply() = new T4()

  class T4 extends T[String, Unit] {

    override def apply(s: String): Unit = println(s)

  }

}

object Misc extends App {

  //  val t = {T1() ~> T2()}

  T4()("test")

  val t = C(C(C(T1(), T2()),T3()),T4())
  t("test2")

  val p = T1() ~> T2() ~> T3() ~> T4()
  p("test3")





}

object Misc2 extends App {

  val args_ = Array("""println("hello world")""",
"""
val p = T1() ~> T2() ~> T3() ~> T4()
p("test3")
"""
  )



//  // redirect to invoking the standard scala main method
//  val method = Class.forName("scala.tools.nsc.MainGenericRunner").
//    getMethod("main", classOf[Array[String]]);

//  // augmented arguments
//  val aurg : Object = (
//    List[String](
//      "-nocompdaemon",
//      "-classpath", System.getProperty("java.class.path"),
//      "-usejavacp"
//    ) ++ args_
//    ).toArray[String];
//
//  method.invoke(null, aurg);

  import scala.tools.nsc.interpreter._
  val s = new Settings()
  s.usejavacp.value = true
  val m = new IMain(s)

  m.interpret(args_(1))

}
