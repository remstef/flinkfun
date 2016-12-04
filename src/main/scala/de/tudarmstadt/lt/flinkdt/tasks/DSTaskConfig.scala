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
import com.typesafe.config.{ConfigRenderOptions, ConfigFactory, Config}
import java.io.File
import de.tudarmstadt.lt.utilities.TimeUtils
import org.apache.flink.core.fs.Path
import scala.collection.JavaConversions._

/**
  * Created by Steffen Remus.
  *
  * from classic JoBimText:
  *
  * min_ndot1    == -f       (2)
  * min_n1dot    == -w       (2)
  * min_n11      == -wf      (2)
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
  * TODO: add option for JoinHints (expected size of dataset)
  *
  */
@SerialVersionUID(42l)
object DSTaskConfig extends Serializable {

  def appendPath(url:String,path:String) = url + (if (url.endsWith("/")) "" else "/") + path

//  def getFullPath(url:String,path:String) = appendPath(url, jobname + "/" + (if (flipct) "flipped-" else "" ) + path)

  @transient
  var config:Config                         = ConfigFactory.load()

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

  var jobname:String                        = null

  var flipct:Boolean                        = false

  var io_text:String                        = "!!NO INPUT DEFINED!!"
  var io_text_column:Int                    = -1
  var io_whitelist:String                   = null

  var io_basedir:String                    = s"file://./${jobname}"
  var io_ctraw:String                      = null
  var io_ctraw_fields:Array[Int]           = null
  var io_accumulated_AB:String             = null
  var io_accumulated_N:String              = null
  var io_accumulated_AB_whitelisted:String = null
  var io_accumulated_A:String              = null
  var io_accumulated_B:String              = null
  var io_accumulated_CT:String             = null
  var io_dt:String                         = null
  var io_dt_sorted:String                  = null
  var io_keymap:String                     = null

  var reread_checkpointed_data:Boolean     = true

  def resolveConfig(args:Array[String] = null): Config = {
    if (args == null || args.length <= 0)
      ConfigFactory.load()
    else {
      val cli_args = ParameterTool.fromArgs(args)
      
      // resolve cli-args
      var conf:Config = ConfigFactory.parseMap(cli_args.toMap)
      
      if (cli_args.has("configfile"))
        conf = conf.withFallback(ConfigFactory.parseFile(new File(cli_args.get("conf")))) // load conf with fallback to default application.conf
      
      if(cli_args.has("conf"))
        conf = conf.withFallback(ConfigFactory.parseString(cli_args.get("conf")))
        
      if(cli_args.has("c"))
        conf = conf.withFallback(ConfigFactory.parseString(cli_args.get("c")))
                
      conf.withFallback(ConfigFactory.parseResources("myapplication.conf")).withFallback(ConfigFactory.load()).resolve()
        
    }
  }

  def load(config:Config) = {
    this.config = config
    val config_dt = config.getConfig("dt")
    if(config_dt.hasPath("jobname"))
      jobname = config_dt.getString("jobname")
    else
      jobname = s"flinkdt-job-${TimeUtils.getSimple17}"

    val ioconfig = config_dt.getConfig("io")

    io_basedir = ioconfig.getString("dir")

    //flipct                         = config_dt.getBoolean("flipct")
    reread_checkpointed_data         = config_dt.getBoolean("re-read-checkpoint-data")

    // get input data and output data
    io_text                        = ioconfig.getString("text")
    io_text_column                 = ioconfig.getString("text-column").toInt
    io_whitelist                   = if(ioconfig.hasPath("whitelist"))           ioconfig.getString("whitelist") else null
    
    val ioct = ioconfig.getConfig("ct")
    
    io_ctraw                      = if(ioct.hasPath("raw"))             ioct.getString("raw")           else null
    io_ctraw_fields               = if(ioct.hasPath("raw-fields"))      ioct.getIntList("raw-fields").map(_.toInt).to[Array]  else null
    io_accumulated_AB             = if(ioct.hasPath("accAB"))           ioct.getString("accAB")         else null
    io_accumulated_N              = if(ioct.hasPath("accN"))            ioct.getString("accN")          else null
    io_accumulated_AB_whitelisted = if(ioct.hasPath("accABwhite"))      ioct.getString("accABwhite")    else null
    io_accumulated_A              = if(ioct.hasPath("accA"))            ioct.getString("accA")          else null
    io_accumulated_B              = if(ioct.hasPath("accB"))            ioct.getString("accB")          else null
    io_accumulated_CT             = if(ioct.hasPath("accall"))          ioct.getString("accall")        else null
    io_dt                         = if(ioct.hasPath("dt"))              ioct.getString("dt")            else null
    io_dt_sorted                  = if(ioct.hasPath("dtsort"))          ioct.getString("dtsort")        else null
    io_keymap                     = if(ioct.hasPath("keymap"))          ioct.getString("keymap")        else null

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

  override def toString() = toString(false)

  def toString(verbose:Boolean = false) = { if(verbose) config else config.getConfig("dt").atKey("dt") }.root().render(ConfigRenderOptions.concise.setFormatted(true).setComments(true).setJson(false))

  def writeConfig(dest:String=null, additional_comments:String = null, verbose:Boolean = false, overwrite:Boolean = false): Unit = {
    val dest_ = if(dest == null || dest.isEmpty) appendPath(io_basedir, s"${jobname}.conf") else dest
    var path:Path = new Path(dest_)
    var i = 0
    while(path.getFileSystem.exists(path))
      path = new Path(dest_ + (i+=1).toString) 
      
    val w = path.getFileSystem.create(path, overwrite)
    if(additional_comments != null && !additional_comments.isEmpty) {
      w.write("# # # \n# \n# additional comments: \n# \n".getBytes)
      for (line <- additional_comments.split('\n'))
        w.write(("#  " + line + '\n').getBytes)
      w.write("# \n# # # \n".getBytes)
    }
    w.write(DSTaskConfig.toString(verbose).getBytes)
    w.flush()
    w.close()
  }

  def jobname(suffix:String): String = s"$jobname-$suffix"

}
