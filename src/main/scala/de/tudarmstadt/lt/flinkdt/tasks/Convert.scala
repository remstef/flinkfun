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

import de.tudarmstadt.lt.flinkdt.textutils.{CtFromString, StringConvert}
import de.tudarmstadt.lt.flinkdt.types._
import de.tudarmstadt.lt.utilities.HashUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

/**
  * Created by Steffen Remus.
  */
object Convert {

  object Hash {

    def StringSha256[CIN <: CT2 : ClassTag : TypeInformation, T1: ClassTag : TypeInformation, T2: ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation](keymap_location: String) = {
      val hf: Any => Array[Byte] = t => HashUtils.string_hash_sha256(t.toString)
      new Convert__Hash[CIN, T1, T2, COUT](hf, hf, keymap_location)
    }

    def StringHashCode[CIN <: CT2 : ClassTag : TypeInformation, T1: ClassTag : TypeInformation, T2: ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation](keymap_location: String) = {
      val hf: Any => Array[Byte] = t => HashUtils.decodeHexString(Integer.toHexString(t.toString.hashCode))
      new Convert__Hash[CIN, T1, T2, COUT](hf, hf, keymap_location)
    }

    def StringMurmur3_32bit[CIN <: CT2 : ClassTag : TypeInformation, T1: ClassTag : TypeInformation, T2: ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation](keymap_location: String) = {
      val hf: Any => Array[Byte] = t => HashUtils.decodeHexString(Integer.toHexString(HashUtils.string_hash_murmur3_32bit(t.toString)))
      new Convert__Hash[CIN, T1, T2, COUT](hf, hf, keymap_location)
    }

    def Reverse[CIN <: CT2 : ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation, T1: ClassTag : TypeInformation, T2: ClassTag : TypeInformation](keymap_location: String) = {
      new ReverseConversion[CIN, COUT, T1, T2](keymap_location)
    }

  }

}


class Convert__Hash[CIN <: CT2 : ClassTag : TypeInformation, T1: ClassTag : TypeInformation, T2: ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation](hashfunA: T1 => Array[Byte], hashfunB: T2 => Array[Byte], keymap_outputlocation: String) extends DSTask[CIN, COUT] {

  override def process(ds: DataSet[CIN]): DataSet[COUT] = {

    val mapStringCtToByteArray = ds.map(ct => {
      val id_A: Array[Byte] = hashfunA(ct.a.asInstanceOf[T1])
      val id_B: Array[Byte] = hashfunB(ct.b.asInstanceOf[T2])
      val newct = getNewCT(ct, id_A, id_B)
      (newct, Seq((ct.a, id_A), (ct.b, id_B)))
    })

    // get mapping Array[Byte] -> String mapping
    val string2id = mapStringCtToByteArray
      .map(_._2)
      .flatMap(l => l)
      .map(t => (StringConvert.convert_toString(t._1), HashUtils.encodeHexString(t._2)))
      .distinct(0)
      .map(t => s"${t._1}\t${t._2}")

    // write mapping
    DSWriter[String](keymap_outputlocation, s"${DSTaskConfig.jobname}-keymap").process(string2id)

    // TODO: whats the best strategy to deal with collisions? Currently we ignore this issue!
    // should we sum the values again? just to be sure they are unique?
    // return int-cts
    mapStringCtToByteArray.map(_._1)

  }

  def getNewCT(ct: CIN, newA: Array[Byte], newB: Array[Byte]): COUT = {
    // go the easy way via serialization and deserialization
    val serialized = ct.toStringArray()
    serialized(0) = StringConvert.convert_toString(newA)
    serialized(1) = StringConvert.convert_toString(newB)
    val deserialized = CtFromString.fromStringArray[COUT, Array[Byte], Array[Byte]](serialized)
    return deserialized
  }

}


class ReverseConversion[CIN <: CT2 : ClassTag : TypeInformation, COUT <: CT2 : ClassTag : TypeInformation, T1: ClassTag : TypeInformation, T2: ClassTag : TypeInformation](keymap_location: String) extends DSTask[CIN, COUT] {

  override def process(ds: DataSet[CIN]): DataSet[COUT] = {
    val id2string = DSReader(keymap_location)
      .process()
      .map(l => l.split('\t') match {
        case Array(string, id, _*) => (HashUtils.decodeHexString(id), string)
        case _ => (Array[Byte](0.toByte), "")
      })

    val converted = ds
      .join(id2string).where("a").equalTo(0)((ct, tup) => (ct, tup._2))
      .join(id2string).where("_1.b").equalTo(0)((ct_tup, tup) => getNewCT(ct_tup._1, ct_tup._2, tup._2))
    converted
  }

  def getNewCT(ct: CIN, newA: String, newB: String): COUT = {
    // go the easy way via serialization and deserialization
    val serialized = ct.toStringArray()
    serialized(0) = newA
    serialized(1) = newB
    val deserialized = CtFromString.fromStringArray[COUT, T1, T2](serialized)
    return deserialized
  }

}
