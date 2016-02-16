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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

import scala.reflect.ClassTag


/**
  * Created by Steffen Remus
  */
object DSWriter {

  def apply[T : ClassTag : TypeInformation](out:String, jobname:String = null) = new DSWriter[T](out,jobname)

}

class DSWriter[T : ClassTag : TypeInformation](out:String, jobname:String) extends DSTask[T,T] {

  override def fromCheckpointLines(lineDS: DataSet[String]): DataSet[T] = ???

  override def fromInputLines(lineDS: DataSet[String]): DataSet[T] = ???

  override def process(ds: DataSet[T]): DataSet[T] = {
    if (out == null)
      return ds
    if(out == "stdout")
      ds.print() // calls x.toString()
    else
      ds.writeAsText(out) // calls x.toString()
    // execute plan
    ds.getExecutionEnvironment.execute(if(jobname == null) DSTaskConfig.jobname else jobname)
    ds
  }

}
