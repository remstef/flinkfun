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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
class Checkpoint[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation](f:DSTask[I,O], out:String = null, jobname:String) extends DSTask[I,O] {

  val output_path:Path = new Path(out)

  override def fromInputLines(lineDS: DataSet[String]): DataSet[I] = f.fromInputLines(lineDS)

  override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[O] = f.fromCheckpointLines(lineDS)

  override def process(ds: DataSet[I]): DataSet[O] = {
    if(output_path.getFileSystem.exists(output_path))
      return fromCheckpointLines(DSReader(out).process())
    return f(ds)
  }

}
