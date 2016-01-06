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

import de.tudarmstadt.lt.flinkdt.types.{CtFromString, CT2def, CT2red}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus
  */
object WhiteListFilter {

  def CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](whitelist:String, env:ExecutionEnvironment, extended_resolution:Boolean = true) = new WhiteListFilter__CT2[T1,T2](whitelist, env, extended_resolution)

  def CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](whitelist:String, env:ExecutionEnvironment, extended_resolution:Boolean = true) = new WhiteListFilter__CT2Min[T1,T2](whitelist, env, extended_resolution)

}


/**
  * Get all contexts and 1-hop transitive words of whitelisted terms.
  * In case of extended_resolution get also the contexts of transitively resolved terms.
  * (Might be needed for correct computation of significance scores / association measures)
  *
  * @param whitelist
  * @param env
  * @param extended_resolution
  * @tparam T1
  * @tparam T2
  */
class WhiteListFilter__CT2[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](whitelist:String, env:ExecutionEnvironment, extended_resolution:Boolean = true) extends DSTask[CT2def[T1, T2],CT2def[T1, T2]] {

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2def[T1,T2]] = lineDS.map(CtFromString[CT2def[T1,T2], T1, T2](_))

  override def process(ds: DataSet[CT2def[T1,T2]]): DataSet[CT2def[T1,T2]] = {

    if(whitelist == null)
      return ds

    val whiteterms = ( if (whitelist.contains('\n')) env.fromCollection(whitelist.split('\n')) else env.readTextFile(whitelist) )
      .filter(s => s.trim.length > 0)
      .map(Tuple1(_))
      .distinct(0)

    val ds_string = ds
      .map(ct => (ct, ct.a.toString, ct.b.toString))

    val white_cts_A = ds_string // get all contexts of whitelist terms
      .joinWithTiny(whiteterms)
      .where(1).equalTo(0)((x, y) =>  x )
      .distinct(2)

    val white_cts_B_from_white_cts_A = ds_string
      .join(white_cts_A)
      .where(2).equalTo(2)((x,y) => x) // get all terms of contexts of whitelist terms

    // result
    if(extended_resolution){
      val white_cts_A_from_white_cts_B = ds_string
        .join(white_cts_B_from_white_cts_A.distinct(1))
        .where(1).equalTo(1)((x,y) => x) // now get all the contexts of the new terms
      white_cts_A_from_white_cts_B.map(_._1)
    }else{
      white_cts_B_from_white_cts_A.map(_._1)
    }

  }

}

class WhiteListFilter__CT2Min[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](whitelist:String, env:ExecutionEnvironment, extended_resolution:Boolean = true) extends DSTask[CT2red[T1, T2],CT2red[T1, T2]] {

  @transient
  val whitelistFilterWrapped = new WhiteListFilter__CT2[T1,T2](whitelist, env, extended_resolution)

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2red[T1,T2]] = lineDS.map(CtFromString[CT2red[T1,T2], T1, T2](_))

  override def process(ds: DataSet[CT2red[T1,T2]]): DataSet[CT2red[T1,T2]] = {
    val ds_ct2:DataSet[CT2def[T1, T2]] = ds.map(_.asCT2Full())
    whitelistFilterWrapped.process(ds_ct2).map(_.toCT2Min())
  }

}

