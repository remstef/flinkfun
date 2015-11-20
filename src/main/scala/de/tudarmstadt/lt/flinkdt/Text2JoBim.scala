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
object Text2JoBim {

  val fixed_indexes = Array[Int](2) // fix the second entry such that the jo will always be the middle one (a,b,@,d,e)
  val make_patterns = PatternGenerator.STRING_GENERATOR
  val pat = new PatGen("*")

  def patterns[T](w:List[T], i:Int):List[List[T]] = new PatternGenerator().get_raw_patterns(w, i, fixed_indexes).map(_.toList).toList

  def patternsall[T](w:List[T], max:Int):TraversableOnce[List[T]] ={
    var s = Traversable[List[T]]()
    for(i <- 0 to max)
      s ++= patterns(w, i)
    return s
  }

  def patterns(text:String): TraversableOnce[JoBim] = {
    ("^ ^ " + text + " $ $").split("\\s+")
      .sliding(5)
      .flatMap(x => patternsall(x.toList, 3))
      .map(x => JoBim(x(2), x.slice(0,2).mkString(" ") + " @ "  + x.slice(3,5).mkString(" ")))
      .map(jb => jb.copy(bim=make_patterns.merge_wildcards_pattern(jb.bim.split(" ").toList).mkString(" ")))
      .filter(jb => !(jb.bim.startsWith("^") || jb.bim.startsWith("^ ^") || jb.bim.startsWith("* ^") || jb.bim.endsWith("$") || jb.bim.endsWith("$ $") || jb.bim.endsWith("$ *")))
  }

  def nGrams(text:String, n:Int): TraversableOnce[JoBim] = {
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    seq.sliding(n)
      .map(x => JoBim(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def kSkipNgram(text:String, n:Int=3, k:Int=2): TraversableOnce[JoBim] = {
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat.kSkipNgrams(seq, n, k)
      .map(x => JoBim(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def patterns_new(text:String, num_wildcards:Int=3): TraversableOnce[JoBim] = {
    val n = num_wildcards
    val nh = n/2;
    val seq = ("^ "*(nh) + text + " $"*(nh)).split("\\s+")
    pat.merged_patterns(seq, num_wildcards).map(_.pattern)
      .map(x => JoBim(x(nh), x.slice(0,nh).mkString(" ") + " @ "  + x.slice(n-nh,n).mkString(" ")))
  }

  def main(args: Array[String]) {
    println("--- ngrams n=3 ---")
    Text2JoBim.nGrams("insurgents killed in ongoing fighting",3).foreach(a => println(a))
    println("--- kskipngrams n=3 k=2 ---")
    Text2JoBim.kSkipNgram("insurgents killed in ongoing fighting",3,2).foreach(a => println(a))

    println("--- patterns old ---")
    Text2JoBim.patterns("The quick brown fox jumps over the lazy dog").filter(_.jo equals "fox").foreach(a => println(a))

//    Text2JoBim.threeGrams("The quick brown fox jumps over the lazy dog").filter(_.jo equals "fox").foreach(a => println(a))

//    val writer = File("test.tsv").printWriter()
//    File("/Volumes/ExtendedHD/Users/stevo/Documents/corpora/simplewiki/simplewikipedia_sent_tok.txt")
//      .lines()
//      .flatMap(line => patterns(line))
//      .foreach(writer.println(_))
    


  }

}

