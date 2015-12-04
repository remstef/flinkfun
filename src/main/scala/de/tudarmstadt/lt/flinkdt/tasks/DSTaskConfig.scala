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

import java.io.File



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
  * topn_f       == -p       (1000)
  * topn_s       == -t       (200)
  * min_sim      == -ms      (2)
  *
  * exclusively for jbtct:
  * min_sim_distinct    (2) // number of distinct similar entries including the Jo itself
  * [min_docs           (1)]
  *
  */
@SerialVersionUID(42l)
object DSTaskConfig extends Serializable{

  /* 2 */
  var param_min_ndot1:Float                 = 2f
  var param_min_n1dot:Float                 = 2f
  var param_min_n11:Float                   = 2f
  var param_min_odot1:Float                 = 2f
  var param_max_odot1:Float                 = 1000f

  var param_min_sig:Float                   = 0f
  var param_topn_f:Int                      = 1000
  var param_topn_s:Int                      = 200
  var param_min_sim:Float                   = 2f
  var param_min_sim_distinct:Int            = 2

  var jobname:String                        = "DTJOB"

  var in_text:String                        = "!!NO INPUT DEFINED!!"
  var in_whitelist:String                   = null

  var out_basedir:String                    = new File("./",jobname).getAbsolutePath
  var out_raw:String                        = null
  var out_accumulated_AB:String             = null
  var out_accumulated_AB_whitelisted:String = null
  var out_accumulated_A:String              = null
  var out_accumulated_B:String              = null
  var out_accumulated_CT:String             = null
  var out_dt:String                         = null
  var out_dt_sorted:String                  = null


  def load(args:Array[String], jobname:String = null, caller:Class[_] = null) = {

    import com.typesafe.config.{ConfigFactory, Config}

    val config:Config =
      if(args.length > 0)
        ConfigFactory.parseFile(new File(args(0))).withFallback(ConfigFactory.load()).resolve() // load conf with fallback to default application.conf
      else
        ConfigFactory.load() // load default application.conf

    if(jobname != null)
      this.jobname = jobname
    else if(caller != null)
      this.jobname = caller.getSimpleName.replaceAllLiterally("$","")

    val config_dt = config.getConfig("DT")
    val outputconfig = config_dt.getConfig("output.ct")
    val outputbasedirfile = new File(if(config_dt.hasPath("output.basedir")) config_dt.getString("output.basedir") else "./", s"out-${this.jobname}")
    if(!outputbasedirfile.exists())
      outputbasedirfile.mkdirs()
    out_basedir = outputbasedirfile.getAbsolutePath

    // get input data and output data
    in_text                        = config_dt.getString("input.text")
    in_whitelist                   = if(config_dt.hasPath("input.whitelist"))    config_dt.getString("input.whitelist") else null
    out_raw                        = if(outputconfig.hasPath("raw"))             new File(out_basedir, outputconfig.getString("raw")).getAbsolutePath else null
    out_accumulated_AB             = if(outputconfig.hasPath("accAB"))           new File(out_basedir, outputconfig.getString("accAB")).getAbsolutePath else null
    out_accumulated_AB_whitelisted = if(outputconfig.hasPath("accABwhite"))      new File(out_basedir, outputconfig.getString("accABwhite")).getAbsolutePath else null
    out_accumulated_A              = if(outputconfig.hasPath("accA"))            new File(out_basedir, outputconfig.getString("accA")).getAbsolutePath else null
    out_accumulated_B              = if(outputconfig.hasPath("accB"))            new File(out_basedir, outputconfig.getString("accB")).getAbsolutePath else null
    out_accumulated_CT             = if(outputconfig.hasPath("accall"))          new File(out_basedir, outputconfig.getString("accall")).getAbsolutePath else null
    out_dt                         = if(outputconfig.hasPath("dt"))              new File(out_basedir, outputconfig.getString("dt")).getAbsolutePath else null
    out_dt_sorted                  = if(outputconfig.hasPath("dtsort"))          new File(out_basedir, outputconfig.getString("dtsort")).getAbsolutePath else null

  }

}
