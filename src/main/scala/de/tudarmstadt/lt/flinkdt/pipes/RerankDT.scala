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

package de.tudarmstadt.lt.flinkdt.pipes

import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.types.{CT2red, CT2, CT2def}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus
  */
object RerankDT extends App {

  var config = DSTaskConfig.resolveConfig(args)
  if(!config.hasPath("dt.jobname"))
    config = DSTaskConfig.resolveConfig(args ++ Array("-dt.jobname", getClass.getSimpleName.replaceAllLiterally("$","")))
  DSTaskConfig.load(config)

  // input data is output of dt computation
  val in = DSTaskConfig.io_dt
  val hash = DSTaskConfig.jobname.toLowerCase.contains("hash")

  val ct_computation_chain =
    if(hash) {
      {
        /* */
        ComputeCT2[CT2red[Array[Byte],Array[Byte]], CT2def[Array[Byte],Array[Byte]], Array[Byte],Array[Byte]](prune = true, sigfun = _.lmi, order = Order.DESCENDING) ~> DSWriter(DSTaskConfig.io_accumulated_CT,s"${DSTaskConfig.io_dt_sorted}-rerank") ~>
        /* */
        Convert.Hash.Reverse[CT2def[Array[Byte], Array[Byte]], CT2def[String, String], String, String](DSTaskConfig.io_keymap)
        /* */
      }
    } else {
      {
        /* */
        ComputeCT2[CT2def[String,String], CT2def[String,String], String,String](prune = true, sigfun = _.lmi, order = Order.DESCENDING) ~> DSWriter(DSTaskConfig.io_accumulated_CT,s"${DSTaskConfig.io_dt_sorted}-rerank")
        /* */
      }
    }

  val rerank_chain = ct_computation_chain ~> FilterSortDT[CT2def[String, String],String, String](_.lmi)

  rerank_chain.process(input = DSTaskConfig.io_dt, output = s"${DSTaskConfig.io_dt_sorted}-rerank")

}
