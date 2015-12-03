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
  *
  * min_docs     (1)
  *
  */
@SerialVersionUID(42l)
object DSTaskConfig extends Serializable{

  var min_ndot1:Float = 2f
  var min_n1dot:Float = 2f
  var min_n11:Float   = 2f
  var min_odot1:Float = 2f
  var max_odot1:Float = 1000f

  var min_sig:Float   = 0f
  var topn_f:Int      = 1000
  var topn_s:Int      = 200
  var min_sim:Float   = 2f

  var jobname:String = "DTJOB"
  var outputbasedir:String = new File("./",jobname).getAbsolutePath
  var input:String = "!!NO INPUT DEFINED!!"
  var raw_output:String = null
  var accumulated_AB_output:String = null
  var accumulated_AB_whitelisted_output:String = null
  var accumulated_A_output:String = null
  var accumulated_B_output:String = null
  var accumulated_CT_output:String = null
  var dt_output:String = null
  var dt_sorted_output:String = null

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
    outputbasedir = outputbasedirfile.getAbsolutePath

    // get input data and output data
    input                             = config_dt.getString("input.text")
    raw_output                        = new File(outputbasedir, outputconfig.getString("raw")).getAbsolutePath
    raw_output                        = new File(outputbasedir, outputconfig.getString("raw")).getAbsolutePath
    accumulated_AB_output             = new File(outputbasedir, outputconfig.getString("accAB")).getAbsolutePath
    accumulated_AB_whitelisted_output = new File(outputbasedir, outputconfig.getString("accABwhite")).getAbsolutePath
    accumulated_A_output              = new File(outputbasedir, outputconfig.getString("accA")).getAbsolutePath
    accumulated_B_output              = new File(outputbasedir, outputconfig.getString("accB")).getAbsolutePath
    accumulated_CT_output             = new File(outputbasedir, outputconfig.getString("accall")).getAbsolutePath
    dt_output                         = new File(outputbasedir, outputconfig.getString("dt")).getAbsolutePath
    dt_sorted_output                  = new File(outputbasedir, outputconfig.getString("dtsort")).getAbsolutePath

  }

}
