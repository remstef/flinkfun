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

package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.CT2Min
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by Steffen Remus.
  */
object ComputeGraphDT {

  def freq[T1 : TypeInformation, T2 : TypeInformation]() = new ComputeGraphDT__Freq[T1,T2]()

}

class ComputeGraphDT__Freq[T1 : TypeInformation, T2 : TypeInformation] extends DSTask[CT2Min[T1,T2],CT2Min[T1,T1]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1,T2]] = lineDS.map(l => CT2Min.fromString[T1,T2](l))

  override def process(ds: DataSet[CT2Min[T1,T2]]): DataSet[CT2Min[T1,T1]] = {

    val adjacencyListsRev = ds
      .groupBy("b")
      .reduceGroup((iter, out:Collector[CT2Min[T1, T1]]) => {
        val l = iter.map(_.a).toIterable
        // TODO: check if collecting the single ct2 or the sequence of ct2s is better
        // TODO: check if this could be optimized due to symmetry
        l.foreach(a => l.foreach(b => out.collect(CT2Min[T1,T1](a, b))))
      })

    val dt = adjacencyListsRev
      .groupBy("a","b")
      .sum("n11")

    dt

  }

}


