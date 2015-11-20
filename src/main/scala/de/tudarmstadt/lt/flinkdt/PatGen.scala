package de.tudarmstadt.lt.flinkdt

import scala.collection.mutable

/**
  * Created by sr on 11/20/15.
  */
@SerialVersionUID(42L)
class PatGen[O](wildcard:O) extends Serializable {

  val NO_FIXED:Array[Int] = Array()

  def comb(n:Int, k:Int, fixed:Seq[Int]) = Range(0,n).combinations(k).filter(_.intersect(fixed).isEmpty);

  def raw_patterns(seq: Seq[O], num_wildcards:Int = 1, fixed_indices:Seq[Int] = NO_FIXED):TraversableOnce[Seq[O]] = {
    val index_combinations = comb(seq.length, num_wildcards, fixed_indices)
    val patterns = for(comb <- index_combinations) yield {
      var pattern = seq
      for(i <- comb)
        pattern = pattern.updated(i, wildcard)
      pattern
    }
    patterns
  }

  def merged_patterns(seq: Seq[O], num_wildcards:Int = 1, fixed_indices:Seq[Int] = NO_FIXED, remove_leading_wildcards:Boolean = true, remove_trailing_wildcards:Boolean = true):TraversableOnce[Seq[O]] = {
    if(num_wildcards < 1)
      return Seq(seq)
    val rpatterns = raw_patterns(seq, num_wildcards, fixed_indices)
    val patterns = rpatterns.map(remove_leading_and_trailing_wildcards(_, remove_leading_wildcards, remove_trailing_wildcards)).map(merge_wildcards(_))
    patterns
  }

  def merge_wildcards(pattern:Seq[O]): Seq[O] = {
    var merged:Seq[O] = pattern.getClass.newInstance()
    merged :+= pattern(0)
    for(i <- 1 until pattern.length if !(wildcard == pattern(i) && wildcard == pattern(i-1))) // for(i = first+1; i <= last; i++)
      merged :+= pattern(i)
    merged
  }

  def remove_leading_and_trailing_wildcards(pattern:Seq[O], remove_leading_wildcards:Boolean=true, remove_trailing_wildcards:Boolean=true):Seq[O] = {
    if(!(remove_leading_wildcards || remove_trailing_wildcards)) // both false
      return pattern
    var first:Int = 0
    while (remove_leading_wildcards && (wildcard == pattern(first)))
      first += 1
    var last:Int = pattern.length - 1
    while (remove_trailing_wildcards && (wildcard == pattern(last)))
      last -= 1
    pattern.slice(first, last+1)
  }

  def skip_patterns(seq: Seq[O], skip:Int = 1):TraversableOnce[Seq[O]] = {
    val rpatterns = raw_patterns(seq, skip, NO_FIXED)
    val patterns = rpatterns.map(remove_leading_and_trailing_wildcards(_, true, true)).map(merge_wildcards(_))
    val skipgrams = patterns.map(remove_wildcards(_))
    skipgrams
  }

  def skip_grams(seq: Seq[O], n:Int = 3, skip:Int = 1):TraversableOnce[Seq[O]] = {
    var rpatterns:mutable.Set[Seq[O]] = mutable.Set()
    for(ngram <- seq.sliding(n+skip))
      rpatterns ++= raw_patterns(ngram, skip, NO_FIXED).map(remove_leading_and_trailing_wildcards(_,true,true))
    val patterns = rpatterns.toSeq.map(merge_wildcards(_))
    val skipgrams = patterns.map(remove_wildcards(_))
    skipgrams
  }

  def remove_wildcards(pattern:Seq[O]): Seq[O] = pattern.filter(_ != wildcard)

}
