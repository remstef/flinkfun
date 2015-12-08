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

package de.tudarmstadt.lt.flinkdt

import java.io.{FileReader, BufferedReader}
import java.util

import de.tudarmstadt.lt.scalautils.PatGen
import de.tudarmstadt.lt.util.PatternGenerator
import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}

import scala.collection.mutable.ListBuffer
import scala.reflect.internal.util.HashSet
import scala.reflect.io.File

/**
 * Created by Steffen Remus.
 */
object TextToCT2 {

  val pat = new PatGen[String]("*")

  val pat_hole = new PatGen[String]("@")

  def ngrams(text:String, n:Int=5): TraversableOnce[CT2Min[String,String]] = {
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    seq.sliding(n)
      .map(x => CT2Min(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def ngram_patterns(text:String, n:Int=5, num_wildcards:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val f = Array(n/2) // 5/2 = 2 => 0 1 @ 3 4
    val ngram_jbs = ngrams(text, n)
    val jb = ngram_jbs.flatMap(ct => pat.merged_patterns(ct.b.split(" "), num_wildcards, f).map(pat => pat.pattern).map(p => CT2Min(a=ct.a, b=p.mkString(" "), ct.n11)))
    jb
  }

  def kSkipNgram(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val nh = n/2; // 3/2 = 1 => 0 @ 2
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat.kSkipNgrams(seq, n, k)
      .map(x => CT2Min(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def kWildcardNgramPatterns(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val nh = n/2; // 3/2 = 1 => 0 @ 2 || 5/2 = 2 => 0 1 @ 4 5
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat_hole.kWildcardNgramPatterns(seq, n, k)
      .map(p => CT2Min(p.filler.mkString(" "),p.pattern.mkString(" "), 1f))
  }


  def kWildcardNgramPatternsPlus(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat_hole
      .kWildcardNgramPatternsPlus(seq, n, k)
      .map(p => CT2Min(p.filler.mkString(" "), compress(p.pattern.toList).mkString(" "), 1f))
  }

  def compress[T](l : List[T]) : List[T] = l match {
    case head::next::tail if (head == next) => compress(next::tail)
    case head::tail => head::compress(tail)
    case nil => List()
  }

}

