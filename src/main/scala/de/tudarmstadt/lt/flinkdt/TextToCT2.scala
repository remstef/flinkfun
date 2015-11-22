package de.tudarmstadt.lt.flinkdt

import java.io.{FileReader, BufferedReader}
import java.util

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

  def ngrams(text:String, n:Int): TraversableOnce[CT2[String]] = {
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    seq.sliding(n)
      .map(x => CT2(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def ngram_patterns(text:String, n:Int=5, num_wildcards:Int=2): TraversableOnce[CT2[String]] = {
    val f = Array(n/2)
    val ngram_jbs = ngrams(text, n)
    val jb = ngram_jbs.flatMap(ct => pat.merged_patterns(ct.B.split(" "), num_wildcards, f).map(pat => pat.pattern).map(p => ct.copy(B=p.mkString(" "))))
    jb
  }

  def kSkipNgram(text:String, n:Int=3, k:Int=2): TraversableOnce[CT2[String]] = {
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat.kSkipNgrams(seq, n, k)
      .map(x => CT2(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def main(args: Array[String]) {
    println("--- ngrams n=3 ---")
    TextToCT2.ngrams("insurgents killed in ongoing fighting",3).foreach(a => println(a))
    println("--- ngram patterns n=5 wildcards=2 ---")
    TextToCT2.ngram_patterns("The quick brown fox jumps over the lazy dog").filter(_.A.equals("fox")).foreach(a => println(a))
    println("--- kskipngrams n=3 k=2 ---")
    TextToCT2.kSkipNgram("insurgents killed in ongoing fighting",3,2).foreach(a => println(a))

//    val writer = File("test.tsv").printWriter()
//    File("/Volumes/ExtendedHD/Users/stevo/Documents/corpora/simplewiki/simplewikipedia_sent_tok.txt")
//      .lines()
//      .flatMap(line => patterns(line))
//      .foreach(writer.println(_))

  }

}

