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

  def threeGrams(text:String): TraversableOnce[JoBim] = {
    ("^ " + text + " $").split("\\s+")
      .sliding(3)
      .map{ x => JoBim(x(1), x(0) + " @ "  + x(2))}
  }

  def nGram[T](seq:Seq[T], n:Int=3, placeholder_begin:T=None, placeholder_end:T=None): TraversableOnce[Seq[T]] = {

//    (Seq.fill(n-1)(placeholder_begin) ++ seq ++ Seq.fill(n-1)(placeholder_end))
    seq
      .sliding(3)

  }



  def kSkipNgram[T](seq:Seq[T], n:Int=3, k:Int=2, placeholder_begin:T=None, placeholder_end:T=None): TraversableOnce[Seq[T]] = {
    val seq_ = seq//(Seq.fill(n-1)(placeholder_begin) ++ seq ++ Seq.fill(n-1)(placeholder_end))


    for(i <- 0 to seq_.length-n; p <- 1 until n if p+n <= seq.length; j <- 0 to k if i+p+j+(n-p) <= seq_.length){
      println(f"index:${i}  fixsize:${p}  skipamount:${j}  ")
      println(f"(b1:${i} e1:${i+p}) ++ (b2:${i+p+j} e2:${i+p+j+(n-p)})")
      print("  ::  ")
      print(seq_.slice(i, i+p))
      print("  ++  ")
      print(seq_.slice(i+p+j, i+p+j+(n-p)))
      print("  --  ")
      print(seq_.slice(i+p, i+p+j))

      println
    }

    val current_skip_n_grams:util.Set[Seq[T]] = new util.HashSet[Seq[T]]()
    for(i <- 0 to seq_.length-n) {
      for (p <- 1 until n if p + n <= seq.length) {
        for (j <- 0 to k if i + p + j + (n - p) <= seq_.length) {
          val skip_n_gram:Seq[T] = seq_.slice(i, i + p) ++ seq_.slice(i + p + j, i + p + j + (n - p))
          current_skip_n_grams.add(skip_n_gram)
        }
      }
    }
    current_skip_n_grams
  }


  def main(args: Array[String]) {
//    Text2JoBim.make_patterns.SORT_OUTPUT = true
//    Text2JoBim.patterns("The quick brown fox jumps over the lazy dog").filter(_.jo equals "fox").foreach(a => println(a))
//
//    Text2JoBim.threeGrams("The quick brown fox jumps over the lazy dog").filter(_.jo equals "fox").foreach(a => println(a))

//    val writer = File("test.tsv").printWriter()
//    File("/Volumes/ExtendedHD/Users/stevo/Documents/corpora/simplewiki/simplewikipedia_sent_tok.txt")
//      .lines()
//      .flatMap(line => patterns(line))
//      .foreach(writer.println(_))
    
    Text2JoBim.kSkipNgram("insurgents killed in ongoing fighting".split("\\s+"),3,2).foreach(a => println(a.mkString(", ")))


//    insurgents killed in
//    insurgents killed ongoing
//    insurgents killed fighting
//    insurgents in ongoing
//    insurgents in fighting
//    insurgents ongoing fighting
//    killed in ongoing
//    killed in fighting
//    killed ongoing fighting
//    in ongoing fighting






    //    insurgents in fighting






//    println(Array('a','b','c').indices)
//    for(i <- 0 until 3) println(i)

  }

}

