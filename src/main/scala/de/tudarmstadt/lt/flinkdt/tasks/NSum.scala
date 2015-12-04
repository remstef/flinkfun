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

import de.tudarmstadt.lt.flinkdt.CT2Min
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus.
  */
class NSum[T1 : TypeInformation, T2:TypeInformation] extends DSTask[CT2Min[T1,T2], CT2Min[T1,T2]]{

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1, T2]] = lineDS.map(CT2Min.fromString[T1,T2](_))

  override def process(ds: DataSet[CT2Min[T1, T2]]): DataSet[CT2Min[T1, T2]] = {
    ???
  }


}
