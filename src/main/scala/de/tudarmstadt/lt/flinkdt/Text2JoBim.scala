package de.tudarmstadt.lt.flinkdt

import de.tudarmstadt.lt.util.PatternGenerator
import scala.collection.JavaConversions._

/**
 * Created by Steffen Remus.
 */
object Text2JoBim {

  var fixed_indexes = Array[Int](2) // fix the second entry such that the jo will always be the middle one (a,b,@,d,e)
  var make_patterns = new PatternGenerator[String]();

  def patterns(w:List[String], i:Int):List[List[String]] = make_patterns.get_raw_patterns(w, i, fixed_indexes).map(_.toList).toList
  def patternsall(w:List[String], max:Int):TraversableOnce[List[String]] ={
    var s = Traversable[List[String]]()
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

  def ngrams(text:String): TraversableOnce[JoBim] = {
    text.split("\\W+")
      .sliding(3)
      .map{ x => JoBim(x(1), x(0) + " @ "  + x(2))}
  }

  def main(args: Array[String]) {
    Text2JoBim.make_patterns.SORT_OUTPUT = true
    Text2JoBim.patterns("The quick brown fox jumps over the lazy dog").filter(_.jo equals "fox").foreach(a => println(a))
  }

}

