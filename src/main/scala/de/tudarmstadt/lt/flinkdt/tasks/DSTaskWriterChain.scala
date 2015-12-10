/*
 * Copyright (c) 2015
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

import de.tudarmstadt.lt.utilities.TimeUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
class DSTaskWriterChain[I:TypeInformation,O:TypeInformation,X:TypeInformation](_f:DSTask[I,X], _g:DSTask[X,O]) extends DSTaskChain[I,O,X](_f,_g) {

  override def fromLines(lineDS: DataSet[String]): DataSet[I] = f.fromLines(lineDS)

  override def process(ds: DataSet[I]): DataSet[O] = {

    var t:DSTask[_,_] = f
    while(t.isInstanceOf[DSTaskChain[_,_,_]] || t.isInstanceOf[DSTaskWriterChain[_,_,_]])
      t = (t.asInstanceOf[DSTaskChain[_,_,_]]).g
    val name = s"${TimeUtils.getSimple17}_${t.getClass.getSimpleName}"
    val out = DSTaskConfig.appendPath(DSTaskConfig.out_basedir, name)

    val ds_intermediate = f.process(ds)
    val w = new DSWriter[String](out)
    w.process(f.toLines(ds_intermediate))
    g(ds_intermediate)

  }

}
