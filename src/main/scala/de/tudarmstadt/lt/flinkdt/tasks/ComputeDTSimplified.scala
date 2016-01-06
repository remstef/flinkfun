package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.types.{CT2, CtFromString, CT2def, CT2red}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus on 12/3/15.
  */
object ComputeDTSimplified {

  def byJoin[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeDTSimplified__BY_JOIN[C,T1,T2]()

  def byGraph[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeDTSimplified__BY_GRAPH[C,T1,T2]()

}

class ComputeDTSimplified__BY_JOIN[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[C, CT2red[T1,T1]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C, T1, T2](_))

  override def process(ds: DataSet[C]): DataSet[CT2red[T1,T1]] = {

    val joined: DataSet[CT2red[T1, T1]] = ds
      .join(ds)
      .where("b")
      .equalTo("b") { (l, r) => CT2red[T1, T1](a = l.a, b = r.a, 1f) }.withForwardedFieldsFirst("a->a").withForwardedFieldsSecond("a->b")

    val dt = joined
      .groupBy("a", "b")
      .sum("n11")

    dt

  }
}


class ComputeDTSimplified__BY_GRAPH[C <: CT2[T1, T2] : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[C, CT2red[T1,T1]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C, T1, T2](_))

  override def process(ds: DataSet[C]): DataSet[CT2red[T1,T1]] = {

    val adjacencyListsRev = ds
      .groupBy("b")
      .reduceGroup((iter, out:Collector[CT2red[T1, T1]]) => {
        val l = iter.map(_.a).toIterable // TODO: can I change this somehow?
        // TODO: check if collecting the single ct2 or the sequence of ct2s is better
        // TODO: check if this could be optimized due to symmetry
        l.foreach(a => l.foreach(b => out.collect(CT2red[T1,T1](a, b, 1f))))
      })

    val dt = adjacencyListsRev
      .groupBy("a","b")
      .sum("n11")

    dt

  }
}


