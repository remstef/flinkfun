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

import de.tudarmstadt.lt.flinkdt.{CT2, CT2Min}
import de.tudarmstadt.lt.utilities.HashUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object Convert {

  object HashCT2MinTypes {

    def StringHashCode[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](keymap_location: String) = {
      val hf: Any => Int = t => t.toString.hashCode
      new Convert__Hash__CT2MinTypes[T1, T2](hf, hf, keymap_location)
    }

    def StringSha256[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](keymap_location: String) = {
      val hf: Any => Int = t => HashUtils.string_hash_sha256(t.toString)
      new Convert__Hash__CT2MinTypes[T1, T2](hf, hf, keymap_location)
    }

    def Reverse[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](env: ExecutionEnvironment, keymap_location: String) = {
      new ReverseConversion__Hash__CT2MinTypes[T1, T2](env, keymap_location)
    }

  }

  object HashCT2Types {

    def StringHashCode[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](keymap_location: String) = {
      val hf: Any => Int = t => t.toString.hashCode
      new Convert__Hash__CT2Types[T1, T2](hf, hf, keymap_location)
    }

    def StringSha256[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](keymap_location: String) = {
      val hf: Any => Int = t => HashUtils.string_hash_sha256(t.toString)
      new Convert__Hash__CT2Types[T1, T2](hf, hf, keymap_location)
    }

    def Reverse[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](env: ExecutionEnvironment, keymap_location: String) = {
      new ReverseConversion__Hash__CT2Types[T1, T2](env, keymap_location)
    }

  }

}

class Convert__Hash__CT2MinTypes[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](hashfunA:T1 => Int, hashfunB:T2 => Int, keymap_outputlocation:String) extends DSTask[CT2Min[T1,T2], CT2Min[Int,Int]]{

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[T1, T2]] = lineDS.map(CT2Min.fromString[T1,T2](_))

  override def process(ds: DataSet[CT2Min[T1, T2]]): DataSet[CT2Min[Int, Int]] = {

    val mapStringCtToInt = ds.map(ct => {
      val id_A:Int = hashfunA(ct.a)
      val id_B:Int = hashfunB(ct.b)
      val newct = CT2Min(id_A, id_B, ct.n11)
      (newct, Seq((ct.a, id_A), (ct.b, id_B)))
    })

    // get mapping Int -> String mapping
    val string2id = mapStringCtToInt
      .map(_._2)
      .flatMap(l => l)
      .map(t => (t._1.toString, t._2))
      .distinct(0)
      .map(t => s"${t._1}\t${t._2}")

    // write mapping
    DSWriter[String](keymap_outputlocation).process(string2id)

    // TODO: whats the best strategy to deal with collisions? Currently we ignore this issue!
    // should we sum the values again? just to be sure they are unique?
    // return int-cts
    mapStringCtToInt.map(_._1)

  }

}

class ReverseConversion__Hash__CT2MinTypes[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](env:ExecutionEnvironment, keymap_location:String) extends DSTask[CT2Min[Int,Int], CT2Min[T1,T2]]{

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2Min[Int, Int]] = lineDS.map(CT2Min.fromString[Int,Int](_))

  override def process(ds: DataSet[CT2Min[Int, Int]]): DataSet[CT2Min[T1, T2]] = {

    val id2string = DSReader(keymap_location, env)
      .process()
      .map(l => l.split('\t') match {
        case Array(string, id, _*) => (id.toInt, string)
        case _ => (0,"")
      })

    val converted = ds
      .join(id2string).where("a").equalTo(0)((ct,tup) => (ct, tup._2))
      .join(id2string).where("_1.b").equalTo(0)((ct_tup,tup) => CT2Min[T1,T2](ct_tup._2.asInstanceOf[T1], tup._2.asInstanceOf[T2], ct_tup._1.n11))

    converted
  }

}


class Convert__Hash__CT2Types[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](hashfunA:T1 => Int, hashfunB:T2 => Int, keymap_outputlocation:String) extends DSTask[CT2[T1,T2], CT2[Int,Int]]{

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[T1, T2]] = lineDS.map(CT2.fromString[T1,T2](_))

  override def process(ds: DataSet[CT2[T1, T2]]): DataSet[CT2[Int, Int]] = {

    val mapStringCtToInt = ds.map(ct => {
      val id_A:Int = hashfunA(ct.a)
      val id_B:Int = hashfunB(ct.b)
      val newct = CT2(id_A, id_B, ct.n11, ct.n1dot, ct.ndot1, ct.n, ct.srcid, ct.isflipped)
      (newct, Seq((ct.a, id_A), (ct.b, id_B)))
    })

    // get mapping Int -> String mapping
    val string2id = mapStringCtToInt
      .map(_._2)
      .flatMap(l => l)
      .map(t => (t._1.toString, t._2))
      .distinct(0)
      .map(t => s"${t._1}\t${t._2}")

    // write mapping
    DSWriter[String](keymap_outputlocation).process(string2id)

    // TODO: whats the best strategy to deal with collisions? Currently we ignore this issue!
    // should we sum the values again? just to be sure they are unique?
    // return int-cts
    mapStringCtToInt.map(_._1)

  }

}

class ReverseConversion__Hash__CT2Types[T1 : ClassTag : TypeInformation, T2 : ClassTag : TypeInformation](env:ExecutionEnvironment, keymap_location:String) extends DSTask[CT2[Int,Int], CT2[T1,T2]]{

  override def fromLines(lineDS: DataSet[String]): DataSet[CT2[Int, Int]] = lineDS.map(CT2.fromString[Int,Int](_))

  override def process(ds: DataSet[CT2[Int, Int]]): DataSet[CT2[T1, T2]] = {

    val id2string = DSReader(keymap_location, env)
      .process()
      .map(l => l.split('\t') match {
        case Array(string, id, _*) => (id.toInt, string)
        case _ => (0,"")
      })
    
    val converted = ds
      .join(id2string).where("a").equalTo(0)((ct,tup) => (ct, tup._2))
      .join(id2string).where("_1.b").equalTo(0)((ct_tup,tup) => CT2[T1,T2](ct_tup._2.asInstanceOf[T1], tup._2.asInstanceOf[T2], ct_tup._1.n11, ct_tup._1.n1dot, ct_tup._1.ndot1, ct_tup._1.n, ct_tup._1.srcid, ct_tup._1.isflipped))

    converted
  }

}
