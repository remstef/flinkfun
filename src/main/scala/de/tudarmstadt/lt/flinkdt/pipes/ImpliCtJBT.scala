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

package de.tudarmstadt.lt.flinkdt.pipes

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

import de.tudarmstadt.lt.flinkdt.tasks._
import de.tudarmstadt.lt.flinkdt.Implicits._
import de.tudarmstadt.lt.flinkdt.textutils.{CtFromString}
import de.tudarmstadt.lt.flinkdt.types.{CT2ext, CT2red}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * Created by Steffen Remus.
  */
object ImpliCtJBT extends App {

  def exec_pipeline(flip:Boolean, reread_checkpointed_data: Boolean): DataSet[CT2red[String,String]] = {

    val suffix = if(flip) "-flipped" else ""

    val ct_raw = env.readCT2r(DSTaskConfig.in_text)

    // BEGIN: compute checkpointed and pruned ct2
    val n11 = ct_raw
      .groupBy("a","b")
      .sum("n11")
      .checkpointed(DSTaskConfig.out_accumulated_AB, CtFromString[CT2red[String,String], String,String], DSTaskConfig.jobname("(1) N11Sum"), reread_checkpointed_data)
      .map(ctr => if(flip) ctr.flipped().asInstanceOf[CT2red[String,String]].asCT2ext() else ctr.asCT2ext())

    val n = n11
      .map(ct => {ct.n = ct.n11; ct.on = 1f; ct})
      .reduce((l,r) => {l.n += r.n; l.on += r.on; l})
      .map(ct => {ct.a = "*"; ct.b = "*"; ct.n11 = 1; ct.n1dot = 1; ct.ndot1 = 1; ct.o1dot = 1; ct.odot1 = 1; ct})
      .checkpointed(DSTaskConfig.out_accumulated_N, CtFromString[CT2ext[String,String], String,String], DSTaskConfig.jobname("(2) NSum"), reread_checkpointed_data)

    val n1dot = n11
      .map(ct => {ct.n1dot = ct.n11; ct.o1dot = 1f; ct})
      .groupBy("a")
      .reduce((l,r) => {l.n1dot += r.n1dot; l.o1dot += r.o1dot; l})
      .map(ct => {ct.b = "*"; ct.n11 = 1; ct.ndot1 = 1; ct.odot1 = 1; ct.n = ct.n1dot; ct.on = ct.o1dot; ct})
      .checkpointed(DSTaskConfig.out_accumulated_A + suffix, CtFromString[CT2ext[String,String], String,String], DSTaskConfig.jobname("(3) [N1dotSum]" + suffix), reread_checkpointed_data)
      .filter(_.n1dot >= DSTaskConfig.param_min_n1dot)

    val ndot1 = n11
      .map(ct => {ct.ndot1 = ct.n11; ct.odot1 = 1f; ct})
      .groupBy("b")
      .reduce((l,r) => {l.ndot1 += r.ndot1; l.odot1 += r.odot1; l}) // .sum("ndot1, odot1")
      .map(ct => {ct.a = "*"; ct.n11 = 1; ct.n1dot = 1; ct.o1dot = 1; ct.n = ct.ndot1; ct.on = ct.odot1; ct})
      .checkpointed(DSTaskConfig.out_accumulated_B + suffix, CtFromString[CT2ext[String,String], String,String], DSTaskConfig.jobname("(4) [Ndot1Sum]" + suffix), reread_checkpointed_data)
      .filter(ct => ct.odot1 <= DSTaskConfig.param_max_odot1 && ct.odot1 >= DSTaskConfig.param_min_odot1)

    val joined_n1dot = n11
      .filter(_.n11 >= DSTaskConfig.param_min_n11)
      .join(n1dot, JoinHint.REPARTITION_SORT_MERGE)
      .where("a").equalTo("a"){(l, r) => { l.n1dot = r.n1dot; l.o1dot = r.o1dot; l }}.withForwardedFieldsFirst("n11; ndot1; odot1; n; on").withForwardedFieldsSecond("n1dot; o1dot")
      .checkpointed(DSTaskConfig.out_accumulated_CT + "_join_n1dot" + suffix, CtFromString[CT2ext[String,String], String,String], DSTaskConfig.jobname("(5.1) [Join N1dot]" + suffix), reread_checkpointed_data)

    val joined = joined_n1dot
      .join(ndot1, JoinHint.REPARTITION_SORT_MERGE)
      .where("b").equalTo("b"){(l, r) => { l.ndot1 = r.ndot1; l.odot1 = r.odot1; l }}.withForwardedFieldsFirst("n11; n1dot; o1dot; n; on").withForwardedFieldsSecond("ndot1; odot1")
      .checkpointed(DSTaskConfig.out_accumulated_CT + "_join_n1dot_ndot1" + suffix, CtFromString[CT2ext[String,String], String,String], DSTaskConfig.jobname("(5.2) [Join N1dot, Ndot1]" + suffix), reread_checkpointed_data)

    val ct2_complete = joined
      .crossWithTiny(n){(ct,n) => {ct.n = n.n; ct.on = n.on; ct}}.withForwardedFieldsFirst("n11; n1dot; ndot1; o1dot; odot1").withForwardedFieldsSecond("n; on")
      .map(ct => (ct, ct.lmi_n)) // sigfun
      .filter(_._2 >= DSTaskConfig.param_min_sig)
      .groupBy("_1.a")
      .sortGroup("_2", Order.ASCENDING)
      .first(DSTaskConfig.param_topn_sig)
      .map(_._1)
      .checkpointed(DSTaskConfig.out_accumulated_CT + suffix, CtFromString[CT2ext[String,String], String,String], DSTaskConfig.jobname("(6) [Join N, Prune by LMI]" + suffix), reread_checkpointed_data)

    // BEGIN: compute DT
    val ct_dt = ct2_complete
      .join(ct2_complete, JoinHint.REPARTITION_SORT_MERGE)
      .where("b")
      .equalTo("b"){(l, r) => CT2red(a = l.a, b = r.a, 1f)}.withForwardedFieldsFirst("a->a").withForwardedFieldsSecond("a->b")
      .checkpointed(DSTaskConfig.out_dt + suffix + "__rawtemp", CtFromString[CT2red[String,String], String,String], DSTaskConfig.jobname("(8) [DT: Join]" + suffix), reread_checkpointed_data)
      .groupBy("a", "b")
      .sum("n11")
      .checkpointed(DSTaskConfig.out_dt + suffix, CtFromString[CT2red[String,String], String,String], DSTaskConfig.jobname("(9) [DT: Sum]" + suffix), reread_checkpointed_data)

    val ct_dt_fsort = ct_dt
      .applyTask(FilterSortDT[CT2red[String, String], String, String](_.n11))
      .checkpointed(DSTaskConfig.out_dt_sorted + suffix, CtFromString[CT2red[String,String], String,String], DSTaskConfig.jobname("10 [DT: Filter, Sort]" + suffix), reread_checkpointed_data)
    // END: dt

    ct_dt_fsort

  }

  DSTaskConfig.load(DSTaskConfig.resolveConfig(args))

  val tf = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ssz")
  val start = System.currentTimeMillis()
  var info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: -- \nduration: -- "
  DSTaskConfig.writeConfig(additional_comments = info)

  implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  exec_pipeline(false, DSTaskConfig.reread_checkpointed_data).first(3).print
  exec_pipeline(true, DSTaskConfig.reread_checkpointed_data).first(3).print

  val end = System.currentTimeMillis()
  val dur = Duration.ofMillis(end-start)
  info = s"main: ${getClass.getName}\nstart: ${tf.format(new Date(start))} \nend: ${tf.format(new Date(end))} \nduration: ${dur.toHours} h ${dur.minusHours(dur.toHours).toMinutes} m ${dur.minusMinutes(dur.toMinutes).toMillis} ms"
  DSTaskConfig.writeConfig(additional_comments = info, overwrite = true)

}
