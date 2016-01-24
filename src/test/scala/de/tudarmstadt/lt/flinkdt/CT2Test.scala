package de.tudarmstadt.lt.flinkdt

import de.tudarmstadt.lt.flinkdt.textutils.TextToCT2
import de.tudarmstadt.lt.flinkdt.types.{CT2red, CT2ext, CT2def}
import org.scalatest.FunSuite

import scala.collection.mutable

/**
  * Created by sr on 11/22/15.
  */
class CT2Test extends FunSuite {

  test("Test consistency") {
    Seq(
      CT2red("a", "b", n11 = 42),
      CT2def("a", "b", n11 = 1, n1dot = 2, ndot1 = 3, n = 10),
      CT2def("a", "b", n11 = 2, n1dot = 1, ndot1 = 3, n = 10), // fail
      CT2def("a", "b", n11 = 3, n1dot = 3, ndot1 = 3, n = 5),
      CT2def("a", "b", n11 = 1, n1dot = 3, ndot1 = 3, n = 3), // fail
      CT2ext("a", "b", n11 = 3, n1dot = 3, ndot1 = 3, n = 4, o1dot = 1, odot1 = 1, on = 1),
      CT2ext("a", "b", n11 = 42, n1dot = 45, ndot1 = 48, n = 142, o1dot = 13, odot1 = 15, on = 27),
      CT2ext("a", "b", n11 = 42, n1dot = 45, ndot1 = 48, n = 142, o1dot = 13, odot1 = 15, on = 23), // fail
      CT2red("a", "b", n11 = 42)
    ).filter(!_.requireConsistency(fail_quietly = true))
      .foreach(ct => println("INCONSISTENT: " + ct))

  }

  test("Test prettyprint") {

    println("--- test pretty print CT2 reduced ---")
    println(CT2red("a", "b", n11 = 42).prettyPrint())


    println("--- test pretty print CT2 default ---")
    println(CT2def("a", "b", n11 = 1, n1dot = 2, ndot1 = 3, n = 10, srcid = Some("mydoc")).prettyPrint())
    println(CT2def("a", "b", n11 = 1, n1dot = 2, ndot1 = 3, n = 10, isflipped = true).prettyPrint())

    println("--- test pretty print CT2 extended ---")
    println(CT2ext("a", "b", n11 = 42, n1dot = 45, ndot1 = 48, n = 142, o1dot = 13, odot1 = 15, on = 27).prettyPrint())

  }

  test("Miscalleanous tests"){

    println("--- ngrams n=3 ---")
    TextToCT2.ngrams("insurgents killed in ongoing fighting", 3).foreach(a => println(a))
    println("--- ngram patterns n=5 wildcards=2 ---")
    TextToCT2.ngram_patterns("The quick brown fox jumps over the lazy dog").filter(_.a.equals("fox")).foreach(a => println(a))
    println("--- kskipngrams n=3 k=2 ---")
    TextToCT2.kSkipNgram("insurgents killed in ongoing fighting", n = 3, k = 2).foreach(a => println(a))
    println("--- kskipngrams n=5 k=3 ---")
    TextToCT2.kSkipNgram("insurgents", n = 5, k = 3).foreach(a => println(a))
    TextToCT2.kSkipNgram("a b c d e f g", 5, 3).foreach(a => println(a))
    println("--- kWildcardsNgramPatterns n=5 k=3 ---")
    TextToCT2.kWildcardNgramPatterns("insurgents", 5, 3).foreach(a => println(a))
    TextToCT2.kWildcardNgramPatterns("a b c d e f g", 5, 3).foreach(a => println(a))

    //    val writer = File("test.tsv").printWriter()
    //    File("/Volumes/ExtendedHD/Users/stevo/Documents/corpora/simplewiki/simplewikipedia_sent_tok.txt")
    //      .lines()
    //      .flatMap(line => patterns(line))
    //      .foreach(writer.println(_))


    println("--- test addition (+) ---")
    val a = CT2def("a", "a", n11 = 1, n1dot = 2, ndot1 = 3, n = 10)
    // test all 4 cases
    val b = CT2def("a", "a", n11 = 1, n1dot = 2, ndot1 = 3, n = 10)
    val c = CT2def("a", "b", n11 = 1, n1dot = 2, ndot1 = 3, n = 10)
    val d = CT2def("b", "a", n11 = 1, n1dot = 2, ndot1 = 3, n = 10)
    val e = CT2def("b", "b", n11 = 1, n1dot = 2, ndot1 = 3, n = 10)

    println(a.prettyPrint())
    println(b.prettyPrint())
    println(s"c  : ${c}")
    println(s"d  : ${d}")
    println(s"e  : ${e}")
    println(s"a+b: ${a + b}")
    println(s"a+c: ${a + c}")
    println(s"a+d: ${a + d}")
    println(s"a+e: ${a + e}")

    println("--- test mutable addition (+=) ---")
    println(s"a   : ${a}")
    println(s"a+=b: ${a += b}")
    println(s"a+=c: ${a += c}")
    println(s"a+=d: ${a += d}")
    println(s"a+=e: ${a += e}")

    println("--- test equals ---")
    println(CT2def("a", "b") == CT2def("a", "b"))
    println(CT2def("a", "b") == CT2def("a", "b", 2))
    println(CT2def("a", "b") == CT2def("b", "a"))

    println("--- test ct2 sets ---")
    val set = Set(CT2def("a", "a"), CT2def("a", "a", 2), CT2def("a", "b"), CT2def("a", "a"), CT2def("a", "b"), CT2def("b", "a"), CT2def("b", "b"))
    println(set.size)
    set.foreach(println _)

    println("--- test ct2 mutable sets ---")
    val mset: mutable.Set[CT2def[String, String]] = mutable.Set()
    mset += CT2def("a", "a")
    mset += CT2def("a", "a", 2)
    mset += CT2def("a", "b")
    mset += CT2def("a", "a")
    mset += CT2def("a", "b")
    mset += CT2def("b", "a")
    mset += CT2def("b", "b")
    println(mset.size)
    mset.foreach(println _)

    println("--- test collapsing ---")
    val ct2s = Seq(CT2def("a", "a", n11 = 2, n1dot = 2, ndot1 = 2, n = 2), CT2def("a", "a"), CT2def("a", "b"), CT2def("a", "a"), CT2def("a", "b"), CT2def("a", "a", n11 = 11, n1dot = 11, ndot1 = 11, n = 11), CT2def("b", "a", n11 = 4, n1dot = 4, ndot1 = 4, n = 4), CT2def("b", "b"))
    println(s"${ct2s.size} == ${ct2s.size}")
    val collapsed_ct2s = Util.collapseCT2(ct2s)
    println(s"${collapsed_ct2s.size} == ${collapsed_ct2s.size}")
    collapsed_ct2s.foreach(x => println(x.prettyPrint()))

    println("--- test collapsing more ---")
    val ct2sm = TextToCT2.ngrams("a a a a a b a a a a a", 3).toIterable
    println(s"${ct2sm.size} == ${ct2sm.size}")
    val collapsed_ct2sm = Util.collapseCT2Min(ct2sm)
    println(s"${collapsed_ct2sm.size} == ${collapsed_ct2sm.size}")
    collapsed_ct2sm.foreach(println _)

    println("--- test Text2CT2 ---")
    TextToCT2.kWildcardNgramPatterns_kplus("a b c d e f g", n = 5, k_max = 2).foreach(println)

    println("--- test Text2CT2 ---")
    TextToCT2.kWildcardNgramPatterns_nplus_kplus("a b c d e f g", n_max = 6, k_max = 2).foreach(println)

  }


}
