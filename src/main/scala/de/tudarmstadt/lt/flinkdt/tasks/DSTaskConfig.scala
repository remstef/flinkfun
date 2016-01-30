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

import org.apache.flink.api.java.utils.ParameterTool
import com.typesafe.config.{ConfigFactory, Config}
import java.io.File
import de.tudarmstadt.lt.utilities.TimeUtils

/**
  * Created by Steffen Remus.
  *
  * from classic JoBimText:
  *
  * min_ndot1    == -f       (2)
  * min_n1dot    == -w       (2)
  * min_n11      == -wf      (2) [classic 0]
  * max_odot1    == -wpfmax  (1000)
  * min_odot1    == -wpfmin  (2)
  *
  * min_sig      == -s       (0)
  * topn_sig     == -p       (1000)
  * topn_sim     == -t       (200)
  * min_sim      == -ms      (2)
  *
  * exclusively for flinkdt:
  * min_sim_distinct    (2) // number of distinct similar entries including the Jo itself
  * [min_docs           (1)] // TODO:
  *
  */
@SerialVersionUID(42l)
object DSTaskConfig extends Serializable{

  def appendPath(url:String,path:String) = url + (if (url.endsWith("/")) "" else "/") + path

  var param_min_ndot1:Float                 = 2f
  var param_min_n1dot:Float                 = 2f
  var param_min_n11:Float                   = 2f
  var param_min_odot1:Float                 = 2f
  var param_max_odot1:Float                 = 1000f

  var param_min_sig:Float                   = 0f
  var param_topn_sig:Int                    = 1000

  var param_min_sim:Float                   = 2f
  var param_min_sim_distinct:Int            = 2
  var param_topn_sim:Int                    = 200

  var jobname:String                        = "flinkjob"

  var in_text:String                        = "!!NO INPUT DEFINED!!"
  var in_text_column:Int                    = -1
  var in_whitelist:String                   = null

  var out_basedir:String                    = s"file://./${jobname}"
  var out_raw:String                        = null
  var out_accumulated_AB:String             = null
  var out_accumulated_AB_whitelisted:String = null
  var out_accumulated_A:String              = null
  var out_accumulated_B:String              = null
  var out_accumulated_CT:String             = null
  var out_dt:String                         = null
  var out_dt_sorted:String                  = null
  var out_keymap:String                     = appendPath(out_basedir, "keymap.tsv")

  def resolveConfig(args:Array[String]): Config = {

    if(args.length <= 0)
      throw new RuntimeException("No configuration parameter.")

    val cli_args = ParameterTool.fromArgs(args)
    val config:Config =
      if(cli_args.has("conf"))
        ConfigFactory.parseMap(cli_args.toMap).withFallback(ConfigFactory.parseFile(new File(cli_args.get("conf"))).withFallback(ConfigFactory.load()).resolve()).resolve() // load conf with fallback to default application.conf
      else
        ConfigFactory.parseMap(cli_args.toMap).withFallback(ConfigFactory.load()).resolve() // load default application.conf
    config

  }

  def load(config:Config) = {

    val config_dt = config.getConfig("DT")

    if(config_dt.hasPath("jobname"))
      jobname = config_dt.getString("jobname")
    else
      jobname = s"${TimeUtils.getSimple17}_${jobname}"

    val outputconfig = config_dt.getConfig("output.ct")

    out_basedir = appendPath(if(config_dt.hasPath("output.basedir")) config_dt.getString("output.basedir") else "./", s"out-${this.jobname}")

    // get input data and output data
    in_text                        = config_dt.getString("input.text")
    in_text_column                 = config_dt.getString("input.text-column").toInt
    in_whitelist                   = if(config_dt.hasPath("input.whitelist"))    config_dt.getString("input.whitelist") else null
    out_raw                        = if(outputconfig.hasPath("raw"))             appendPath(out_basedir, outputconfig.getString("raw")) else null
    out_accumulated_AB             = if(outputconfig.hasPath("accAB"))           appendPath(out_basedir, outputconfig.getString("accAB")) else null
    out_accumulated_AB_whitelisted = if(outputconfig.hasPath("accABwhite"))      appendPath(out_basedir, outputconfig.getString("accABwhite")) else null
    out_accumulated_A              = if(outputconfig.hasPath("accA"))            appendPath(out_basedir, outputconfig.getString("accA")) else null
    out_accumulated_B              = if(outputconfig.hasPath("accB"))            appendPath(out_basedir, outputconfig.getString("accB")) else null
    out_accumulated_CT             = if(outputconfig.hasPath("accall"))          appendPath(out_basedir, outputconfig.getString("accall")) else null
    out_dt                         = if(outputconfig.hasPath("dt"))              appendPath(out_basedir, outputconfig.getString("dt")) else null
    out_dt_sorted                  = if(outputconfig.hasPath("dtsort"))          appendPath(out_basedir, outputconfig.getString("dtsort")) else null
    out_keymap                     = if(outputconfig.hasPath("keymap"))          appendPath(out_basedir, outputconfig.getString("keymap")) else appendPath(out_basedir, "keymap.tsv")

    // get filter config
    val config_filter = config_dt.getConfig("filter")
    param_min_ndot1         = config_filter.getDouble("min-ndot1").toFloat
    param_min_n1dot         = config_filter.getDouble("min-n1dot").toFloat
    param_min_n11           = config_filter.getDouble("min-n11").toFloat
    param_min_odot1         = config_filter.getDouble("min-odot1").toFloat
    param_max_odot1         = config_filter.getDouble("max-odot1").toFloat

    param_min_sig           = config_filter.getDouble("min-sig").toFloat
    param_topn_sig          = config_filter.getInt("topn-sig")
    param_min_sim           = config_filter.getDouble("min-sim").toFloat
    param_min_sim_distinct  = config_filter.getInt("min-sim-distinct")
    param_topn_sim          = config_filter.getInt("topn-sim")

  }

}
