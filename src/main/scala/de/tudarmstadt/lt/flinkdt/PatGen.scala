package de.tudarmstadt.lt.flinkdt

import scala.collection.{mutable}

/**
  * Created by sr on 11/20/15.
  */
@SerialVersionUID(42L)
class PatGen[O](wildcard:O, __debug:Boolean = false) extends Serializable {

  case class Pat(var pattern:Seq[O], indices:Seq[Int], filler:Seq[O])

  private val NO_FIXED:Array[Int] = Array()

  def comb(n:Int, k:Int, fixed:Seq[Int] = NO_FIXED) = if(__debug) comb_ordered(n,k,fixed) else comb_undefined_ordering(n, k, fixed)

  // ordering is not specified (default)
  def comb_undefined_ordering(n:Int, k:Int, fixed:Seq[Int] = NO_FIXED) = Range(0,n).combinations(k).filter(_.intersect(fixed).isEmpty)

  // ordering is enforced (for debugging purposes)
  def comb_ordered(n:Int, k:Int, fixed:Seq[Int] = NO_FIXED) = comb_undefined_ordering(n, k, fixed).toSeq.sortWith((c1, c2) => {
    val result = for(i <- 0 until c1.size) yield c2(i) - c1(i)
    val d = result.find(_ != 0)
    d.getOrElse(0) > 0
  })

  def raw_patterns(seq: IndexedSeq[O], num_wildcards:Int = 1, fixed_indices:Seq[Int] = NO_FIXED):TraversableOnce[Pat] = {
    val index_combinations = comb(seq.length, num_wildcards, fixed_indices)
    val patterns = for(comb <- index_combinations) yield {
      var pattern = seq
      var filler:Seq[O] = IndexedSeq()
      for (i <- comb) {
        filler :+= pattern(i)
        pattern = pattern.updated(i, wildcard)
      }
      Pat(pattern,comb,filler)
    }
    patterns
  }

  def merged_patterns(seq: IndexedSeq[O], num_wildcards:Int = 1, fixed_indices:Seq[Int] = NO_FIXED, remove_leading_wildcards:Boolean = true, remove_trailing_wildcards:Boolean = true):TraversableOnce[Pat] = {
    if(num_wildcards < 1)
      return Seq(seq).map(s => Pat(seq,Array[Int](),Seq[O]()))
    val rpatterns = raw_patterns(seq, num_wildcards, fixed_indices)
    val patterns = rpatterns.map(pat => {
      pat.pattern = remove_leading_and_trailing_wildcards(pat.pattern, remove_leading_wildcards, remove_trailing_wildcards)
      pat.pattern = merge_wildcards(pat.pattern)
      pat
    })
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

  def skip_patterns(seq: IndexedSeq[O], skip:Int = 1):TraversableOnce[Pat] = {
    val rpatterns = raw_patterns(seq, skip, NO_FIXED)
    val skipgrams = rpatterns.map(pat => {
      pat.pattern = remove_leading_and_trailing_wildcards(pat.pattern, true, true)
      pat.pattern = merge_wildcards(pat.pattern)
      pat.pattern = remove_wildcards(pat.pattern)
      pat
    })
    skipgrams
  }

  def kSkipNgrams(seq: IndexedSeq[O], n:Int = 3, skip:Int = 1):TraversableOnce[Seq[O]] = {
    if (seq.length <= n)
      return Set(seq)
    val skip_correct  = Math.min(seq.length-n, skip)
    var rpatterns:mutable.Set[Seq[O]] = mutable.Set()
    for(ngram <- seq.sliding(n+skip_correct))
      rpatterns ++= raw_patterns(ngram, skip_correct, NO_FIXED)
        .map(_.pattern)
        .map(remove_leading_and_trailing_wildcards(_,true,true))
    val patterns = rpatterns.toSeq.map(merge_wildcards(_))
    val skipgrams = patterns.map(remove_wildcards(_))
    skipgrams
  }

  def remove_wildcards(pattern:Seq[O]): Seq[O] = pattern.filter(_ != wildcard)

}
