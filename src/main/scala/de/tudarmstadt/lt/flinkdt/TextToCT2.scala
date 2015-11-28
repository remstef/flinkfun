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

  val pat = new PatGen("*")

  def ngrams(text:String, n:Int=5): TraversableOnce[CT2Min[String,String]] = {
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    seq.sliding(n)
      .map(x => CT2Min(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def ngram_patterns(text:String, n:Int=5, num_wildcards:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val f = Array(n/2) // 5/2 = 2 => 0 1 @ 3 4
    val ngram_jbs = ngrams(text, n)
    val jb = ngram_jbs.flatMap(ct => pat.merged_patterns(ct.B.split(" "), num_wildcards, f).map(pat => pat.pattern).map(p => ct.copy(B=p.mkString(" "))))
    jb
  }

  def kSkipNgram(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    val nh = n/2; // 3/2 = 1 => 0 @ 2
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat.kSkipNgrams(seq, n, k)
      .map(x => CT2Min(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def kWildcardNgramPatterns(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2Min[String,String]] = {
    throw new NotImplementedError("Not yet implemented")
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat.kWildcardNgramPatterns(seq, n, k)
      .map(x => CT2Min(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }



}

