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
import org.apache.flink.core.fs.{Path, FileSystem}
import scala.reflect.ClassTag
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala._
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.io.FileOutputFormat


/**
  * Created by Steffen Remus
  */
object DSWriter {
  
  def apply[T : ClassTag : TypeInformation](out:String, jobname:String = null) = new DSWriter[T](out,jobname)

}

class DSWriter[T : ClassTag : TypeInformation](out:String, jobname:String) extends DSTask[T,T] {

  override def process(ds: DataSet[T]): DataSet[T] = {
    if (out == null)
      return ds
    if(out == "stdout")
      ds.print() // calls x.toString()
    else
      ds.writeAsCsv(out, fieldDelimiter = "\t", writeMode = WriteMode.NO_OVERWRITE);

    // execute plan
    try {
      ds.getExecutionEnvironment.execute(if (jobname == null) DSTaskConfig.jobname else jobname)
    } catch {
      case t:Throwable => {
        // job failed -> clean up data if necessary
        if (out != "stdout") {
          val output_path:Path = new Path(out)
          val fs:FileSystem = output_path.getFileSystem
          if (fs.exists(output_path)) { // if the folder doesn't exist don't do anything
            // rename directory, replace if it existed before
            val failed_output_path:Path = new Path(s"${out}-failed")
            if(fs.exists(failed_output_path))
              fs.delete(failed_output_path, true)
            fs.rename(output_path, failed_output_path)
          }
        }
        // finally re-throw the exception
        throw(t)
      }
    }
    ds
  }

}
