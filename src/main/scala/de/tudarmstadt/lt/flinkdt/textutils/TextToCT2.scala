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

package de.tudarmstadt.lt.flinkdt.textutils

import de.tudarmstadt.lt.flinkdt.tasks.DSTaskConfig
import de.tudarmstadt.lt.flinkdt.types.CT2red
import de.tudarmstadt.lt.scalautils.PatGen

/**
 * Created by Steffen Remus.
 */
object TextToCT2 {

  implicit val wildcard = "*"

  def coocurrence(text:String): TraversableOnce[CT2red[String,String]] = {
    val seq = text.split("\\s+").toSeq
    (for(w1 <- seq; w2 <- seq if w1 != w2) yield CT2red(w1, w2)).map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
  }

  def ngrams(text:String, n:Int=5): TraversableOnce[CT2red[String,String]] = {
    val nh = Math.max(1,n/2)
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    seq.sliding(n)
      .map(x => CT2red(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
      .map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
  }

  def ngram_patterns(text:String, n:Int=5, num_wildcards:Int=2): TraversableOnce[CT2red[String,String]] = {
    val f = Array(n/2) // 5/2 = 2 => 0 1 @ 3 4
    val ngram_jbs = ngrams(text, n)
    val jb = ngram_jbs.
      flatMap(ct => PatGen(ct.b.split(" "))
        .patterns(num_wildcards, f)
        .map(pat => pat.mergedPattern)
        .map(p => CT2red(a=ct.a, b=p.mkString(" "), ct.n11)))
      .map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
    jb
  }

  def kSkipNgram(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2red[String,String]] = {
    val nh = Math.max(1, n/2) // 3/2 = 1 => 0 @ 2
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    PatGen(seq).kWildcardNgramPatterns(n=n+k,k=k)
      .map(_.skipGram)
      .map(x => CT2red(x(nh), x.slice(0,nh-1).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
      .map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
  }

  def kWildcardNgramPatterns(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2red[String,String]] = {
    val nh = Math.max(1,n/2) // 3/2 = 1 => 0 @ 2 || 5/2 = 2 => 0 1 @ 4 5
    val seq = ("^ "*(nh) + text + " $"*(nh)).replaceAllLiterally("@","(at)").split("\\s+")
    PatGen(seq)("@").kWildcardNgramPatterns(n,k)
      .map(p => CT2red(p.mergedPattern.mkString(" "), p.filler.mkString(" "), 1f))
      .filter(_.b != "^")
      .filter(_.b != "$")
      .filter(_.b != "^ ^")
      .filter(_.b != "$ $")
      .map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
  }

  def kWildcardNgramPatterns_kplus(text:String, n:Int=3, k_max:Int=2): TraversableOnce[CT2red[String,String]] = {
    val nh = Math.max(1,n/2)
    val seq = ("^ "*(nh) + text + " $"*(nh)).replaceAllLiterally("@","(at)").split("\\s+")
    PatGen(seq)("@").kWildcardNgramPatterns_kplus(n, k_max)
      .map(p => CT2red(p.mergedPattern.mkString(" "), p.filler.mkString(" "), 1f))
      .filter(_.b != "^")
      .filter(_.b != "$")
      .filter(_.b != "^ ^")
      .filter(_.b != "$ $")
//      .map(ct => {ct.b = ct.b.replaceAllLiterally("^","").replaceAllLiterally(" $",""); ct})
//      .map(ct => {ct.a = ct.a.replaceAllLiterally("^ ","").replaceAllLiterally(" $","").replaceAllLiterally("@ @", "@"); ct})
    // TODO: make replacement rules generic!
      .map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
  }

  def kWildcardNgramPatterns_nplus_kplus(text:String, n_max:Int=3, k_max:Int=2): TraversableOnce[CT2red[String,String]] = {
    val nh = Math.max(1,n_max/2)
    val seq = text.replaceAllLiterally("@","(at)").split("\\s+")
    PatGen(seq)("@").kWildcardNgramPatterns_nplus_kplus(n_max,k_max)
      .map(p => CT2red(p.reversed().mergedPattern.mkString(" "), p.mergedPattern.mkString(" "), 1f))
      .map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
  }

  def ngramPatternWordPairs(text:String, nmax:Int=5): TraversableOnce[CT2red[String,String]] = {
    val seq = text.replaceAllLiterally("@","(at)").split("\\s+")
    if(seq.length < 3) // we need at least two anchor words and one context word
      return Seq.empty[CT2red[String,String]]
    PatGen(seq)("@")
      .kWildcardNgramPatterns_nplus(nmax, k=2)
      .map(p => CT2red(p.pattern.mkString(" "), p.filler.mkString(" "), 1f))
      .map(ct => if (DSTaskConfig.flipct) ct.flipped().asInstanceOf[CT2red[String,String]] else ct)
  }

}

