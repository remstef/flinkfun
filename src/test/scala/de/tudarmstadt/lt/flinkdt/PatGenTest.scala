package de.tudarmstadt.lt.flinkdt

/**
  * Created by sr on 11/20/15.
  */
object PatGenTest {

  def main(args: Array[String]) {
    val p = new PatGen('*')
    val seq = "hello".toCharArray
    var skipgrams:TraversableOnce[Seq[Char]] = None

    for(i <- 1 to 3) {
      println(f"--- raw_patterns ${i} ---")
      p.raw_patterns(seq, num_wildcards = i).foreach(x => println(x.mkString(" ")))
      println(f"--- merged_patterns ${i} ---")
      p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = false, remove_trailing_wildcards = false).foreach(x => println(x.mkString(" ")))
      println(f"--- merged_patterns ${i} (leading & trailing removed [default])---")
      p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = true, remove_trailing_wildcards = true).foreach(x => println(x.mkString(" ")))
      println(f"--- merged_patterns ${i} (leading removed)---")
      p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = true, remove_trailing_wildcards = false).foreach(x => println(x.mkString(" ")))
      println(f"--- merged_patterns ${i} (trailing removed)---")
      p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = false, remove_trailing_wildcards = true).foreach(x => println(x.mkString(" ")))
      println(f"--- skip_patterns ${i} ---")
      p.skip_patterns(seq, skip = i).foreach(x => println(x.mkString(" ")))
    }

    println("--- 3grams ---")
    seq.sliding(3).foreach(x => println(x.mkString(" ")))

    println("--- 1skip3grams (expected 7) ---")
    skipgrams = p.skip_grams(seq, n=3, skip=1)
    skipgrams.foreach(x => println(x.mkString(" ")))
    println(skipgrams.size)

    println("--- 2skip3grams (expected 10) ---")
    skipgrams = p.skip_grams(seq, n=3, skip=2)
    skipgrams.foreach(x => println(x.mkString(" ")))
    println(skipgrams.size)

  }

}
