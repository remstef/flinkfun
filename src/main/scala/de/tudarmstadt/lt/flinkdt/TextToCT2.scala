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

import de.tudarmstadt.lt.scalautils.PatGen

/**
 * Created by Steffen Remus.
 */
object TextToCT2 {

  implicit val wildcard = "*"

  def ngrams(text:String, n:Int=5): TraversableOnce[CT2Min[String,String]] = {
    val nh = Math.max(1,n/2)
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    seq.sliding(n)
      .map(x => CT2Min(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def ngram_patterns(text:String, n:Int=5, num_wildcards:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val f = Array(n/2) // 5/2 = 2 => 0 1 @ 3 4
    val ngram_jbs = ngrams(text, n)
    val jb = ngram_jbs.flatMap(ct => PatGen(ct.b.split(" ")).patterns(num_wildcards, f).map(pat => pat.mergedPattern).map(p => CT2Min(a=ct.a, b=p.mkString(" "), ct.n11)))
    jb
  }

  def kSkipNgram(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val nh = Math.max(1, n/2) // 3/2 = 1 => 0 @ 2
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    PatGen(seq).kWildcardNgramPatterns(n=n+k,k=k)
      .map(_.skipGram)
      .map(x => CT2Min(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def kWildcardNgramPatterns(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val nh = Math.max(1,n/2) // 3/2 = 1 => 0 @ 2 || 5/2 = 2 => 0 1 @ 4 5
    val seq = ("^ "*(nh) + text + " $"*(nh)).replaceAllLiterally("@","(at)").split("\\s+")
    PatGen(seq)("@").kWildcardNgramPatterns(n,k)
      .map(p => CT2Min(p.filler.mkString(" "), p.mergedPattern.mkString(" "), 1f))
      .filter(_.a != "^")
      .filter(_.a != "$")
      .filter(_.a != "^ ^")
      .filter(_.a != "$ $")
  }

  def kWildcardNgramPatternsPlus(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val nh = Math.max(1,n/2)
    val seq = ("^ "*(nh) + text + " $"*(nh)).replaceAllLiterally("@","(at)").split("\\s+")
    PatGen(seq)("@").kWildcardNgramPatternsPlus(n,k)
      .map(p => CT2Min(p.filler.mkString(" "), p.mergedPattern.mkString(" "), 1f))
      .filter(_.a != "^")
      .filter(_.a != "$")
      .filter(_.a != "^ ^")
      .filter(_.a != "$ $")
//      .map(ct => {ct.a = ct.a.replaceAllLiterally("^","").replaceAllLiterally(" $",""); ct})
//      .map(ct => {ct.b = ct.b.replaceAllLiterally("^ ","").replaceAllLiterally(" $","").replaceAllLiterally("@ @", "@"); ct})

    // TODO: make replacement rules generic!
  }

}

