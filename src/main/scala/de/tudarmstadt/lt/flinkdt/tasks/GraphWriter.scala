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

package de.tudarmstadt.lt.flinkdt.tasks

import de.tudarmstadt.lt.flinkdt.textutils.CtFromString
import de.tudarmstadt.lt.flinkdt.types.CT2
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object GraphWriter {

  def apply[CT <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](out:String) = new GraphWriter[CT, T1, T2](out)

}

class GraphWriter[CT <: CT2 : ClassTag : TypeInformation, T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](out:String) extends DSTask[CT,CT] {

  override def fromInputLines(lineDS: DataSet[String]): DataSet[CT] = lineDS.map(CtFromString[CT, T1, T2](_))

  override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[CT] = ???

  override def process(ds: DataSet[CT]): DataSet[CT] = {
    if (out == null)
      return ds
    ds.writeAsText(s"${out}-edges") // calls x.toString()
    ds.flatMap(ct => ct.toStringArray().slice(0,2))
      .distinct()
      .writeAsText(s"${out}-nodes") // calls x.toString()
    ds.getExecutionEnvironment.execute(DSTaskConfig.jobname)
    ds
  }

}