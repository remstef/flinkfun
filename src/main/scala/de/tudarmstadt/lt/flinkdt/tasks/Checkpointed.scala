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
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.core.fs.Path

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object Checkpointed {

  /**
    *
    * @param f
    * @param out
    * @param jobname
    * @param reReadFromCheckpoint if true, the written data is read and provided for upcoming task (this might improve performance on big datasets), otherwise the intermediate results are re-used
    * @tparam I
    * @tparam O
    * @return
    */
  def apply[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation](f:DSTask[I,O], out:String, jobname:String = null, reReadFromCheckpoint:Boolean=false, env:ExecutionEnvironment = null) = new Checkpointed[I,O](f, out, jobname, reReadFromCheckpoint, env)

}

class Checkpointed[I : ClassTag : TypeInformation, O : ClassTag : TypeInformation](f:DSTask[I,O], out:String, jobname:String = null, reReadFromCheckpoint:Boolean = false, env:ExecutionEnvironment = null) extends DSTask[I,O] {

  val output_path:Path = new Path(out)

  override def process(ds: DataSet[I]): DataSet[O] = {
    if(output_path.getFileSystem.exists(output_path))
      return DSReader[O](out, env).process()
    val ds_out = f(ds)
    if(out != null) {
      DSWriter[O](out, jobname).process(ds_out)
      if(reReadFromCheckpoint) // throw away intermediate results and continue to work with the re-read data
        return DSReader[O](out, env).process()
      // else just re-use the processed data
    }
    return ds_out
  }

}
