package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types.{CT2, CT2def, CT2red}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus on 12/3/15.
  */
object ComputeDTSimplified {

  def byJoin[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeDTSimplified__byJoin[C,T1,T2]()

  def byGraph[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeDTSimplified__byGraph[C,T1,T2]()

}

class ComputeDTSimplified__byJoin[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[C, CT2red[T1,T1]] {

  override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[CT2red[T1, T1]] = lineDS.map(CtFromString[CT2red[T1,T1], T1, T1](_))

  override def fromInputLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C, T1, T2](_))

  override def process(ds: DataSet[C]): DataSet[CT2red[T1,T1]] = {

    val joined: DataSet[CT2red[T1, T1]] = ds
      .join(ds, JoinHint.REPARTITION_SORT_MERGE)
      .where("b")
      .equalTo("b") { (l, r) => CT2red[T1, T1](a = l.a.asInstanceOf[T1], b = r.a.asInstanceOf[T1], 1f) }.withForwardedFieldsFirst("a->a").withForwardedFieldsSecond("a->b")

    val dt = joined
      .groupBy("a", "b")
      .sum("n11")

    dt

  }
}


class ComputeDTSimplified__byGraph[C <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[C, CT2red[T1,T1]] {

  override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[CT2red[T1, T1]] = lineDS.map(CtFromString[CT2red[T1,T1], T1, T1](_))

  override def fromInputLines(lineDS: DataSet[String]): DataSet[C] = lineDS.map(CtFromString[C, T1, T2](_))

  override def process(ds: DataSet[C]): DataSet[CT2red[T1,T1]] = {

    val adjacencyListsRev = ds
      .groupBy("b")
      .reduceGroup((iter, out:Collector[CT2red[T1, T1]]) => {
        val l = iter.map(_.a).toIterable // TODO: can I change this somehow?
        // TODO: check if collecting the single ct2 or the sequence of ct2s is better
        // TODO: check if this could be optimized due to symmetry
        l.foreach(a => l.foreach(b => out.collect(CT2red[T1,T1](a.asInstanceOf[T1], b.asInstanceOf[T1], 1f))))
      }).withForwardedFields("a->a; a->b")

    val dt = adjacencyListsRev
      .groupBy("a","b")
      .sum("n11")

    dt

  }
}


