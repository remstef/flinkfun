package de.tudarmstadt.lt.flinkdt

/**
  * Created by sr on 11/20/15.
  */
object PatGenTest extends App {

  val p = new PatGen("*", true)
  println(f"--- unsorted combinations ---")
  p.comb_undefined_ordering(5, 3).foreach(c => println(c.mkString(" ")))
  println(f"--- sorted combinations ---")
  p.comb_ordered(5, 3).foreach(c => println(c.mkString(" ")))

  val seq = "h e l l o".split("\\s+")
  var skipgrams:TraversableOnce[Seq[String]] = None

  for(i <- 1 to 3) {
    println(f"--- raw_patterns ${i} ---")
    p.raw_patterns(seq, num_wildcards = i).foreach(x => println(x))
    println(f"--- merged_patterns ${i} ---")
    p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = false, remove_trailing_wildcards = false).foreach(x => println(x))
    println(f"--- merged_patterns ${i} (leading & trailing removed [default])---")
    p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = true, remove_trailing_wildcards = true).foreach(x => println(x))
    println(f"--- merged_patterns ${i} (leading removed)---")
    p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = true, remove_trailing_wildcards = false).foreach(x => println(x))
    println(f"--- merged_patterns ${i} (trailing removed)---")
    p.merged_patterns(seq, num_wildcards = i, remove_leading_wildcards = false, remove_trailing_wildcards = true).foreach(x => println(x))
    println(f"--- skip_patterns ${i} ---")
    p.skip_patterns(seq, skip = i).foreach(x => println(x))
  }

  println("--- 3grams ---")
  seq.sliding(3).foreach(x => println(x.mkString(" ")))

  println("--- 1skip3grams (expected 7) ---")
  skipgrams = p.kSkipNgrams(seq, n=3, skip=1)
  skipgrams.foreach(x => println(x.mkString(" ")))
  println(skipgrams.size)

  println("--- 2skip3grams (expected 10) ---")
  skipgrams = p.kSkipNgrams(seq, n=3, skip=2)
  skipgrams.foreach(x => println(x.mkString(" ")))
  println(skipgrams.size)

  println("--- skip ngrams test from XYZ et al. ---")
  //*    insurgents killed in
  //*    insurgents killed ongoing
  //*    insurgents killed fighting
  //*    insurgents in ongoing
  //*    insurgents in fighting
  //*    insurgents ongoing fighting
  //*    killed in ongoing
  //*    killed in fighting
  //*    killed ongoing fighting
  //*    in ongoing fighting
  skipgrams = p.kSkipNgrams("insurgents killed in ongoing fighting".split("\\s+"), n=3, skip=2)
  skipgrams.foreach(x => println(x.mkString(" ")))

}
