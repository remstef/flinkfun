package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.types.{CT2Min, CT2}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus on 12/3/15.
  */
object ComputeDTSimplified {

  def CT2Join[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeDTSimplified__fromCT2_join[T1,T2]()

  def CT2MinJoin[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeDTSimplified__fromCT2Min_join[T1,T2]()

  def CT2MinGraph[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation]() = new ComputeDTSimplified__fromCT2Min_graph[T1,T2]()

}

class ComputeDTSimplified__fromCT2Min_join[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2Min[T1,T2], CT2Min[T1,T1]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(l => CT2Min.fromString(l))

  override def process(ds: DataSet[CT2Min[T1, T2]]): DataSet[CT2Min[T1, T1]] = {
    val joined:DataSet[CT2Min[T1,T1]] = ds
      .join(ds)
      .where("b")
      .equalTo("b")((l,r) => CT2Min[T1,T1](l.a, r.a, n11=1f))

    val dt = joined
      .groupBy("a", "b")
      .sum("n11")

    dt
  }

}


class ComputeDTSimplified__fromCT2_join[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2[T1,T2], CT2Min[T1,T1]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1,T2]] = lineDS.map(l => CT2.fromString(l))

  override def process(ds: DataSet[CT2[T1, T2]]): DataSet[CT2Min[T1, T1]] = {
    val joined:DataSet[CT2Min[T1,T1]] = ds
      .join(ds)
      .where("b")
      .equalTo("b")((l,r) => CT2Min[T1,T1](l.a, r.a, n11=1f))

    val dt = joined
      .groupBy("a", "b")
      .sum("n11")

    dt
  }

}

class ComputeDTSimplified__fromCT2Min_graph[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2Min[T1,T1]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(l => CT2Min.fromString[T1,T2](l))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2Min[T1,T1]] = {

    val adjacencyListsRev = ds
      .groupBy("b")
      .reduceGroup((iter, out:Collector[CT2Min[T1, T1]]) => {
        val l = iter.map(_.a).toIterable // TODO: can I change this somehow?
        // TODO: check if collecting the single ct2 or the sequence of ct2s is better
        // TODO: check if this could be optimized due to symmetry
        l.foreach(a => l.foreach(b => out.collect(CT2Min[T1,T1](a, b, 1f))))
      })

    val dt = adjacencyListsRev
      .groupBy("a","b")
      .sum("n11")

    dt

  }

}
