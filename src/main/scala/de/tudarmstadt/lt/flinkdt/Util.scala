package de.tudarmstadt.lt.flinkdt

import de.tudarmstadt.lt.flinkdt.tasks.DSTaskConfig
import de.tudarmstadt.lt.flinkdt.types.{CT2Min, CT2}

import scala.collection.mutable

/**
  * Created by Steffen Remus
  */
object Util {

  def getExtractorfunFromJobname():String => TraversableOnce[CT2Min[String,String]] = {
    if(DSTaskConfig.jobname.contains("pair"))
      TextToCT2.ngramPatternWordPairs(_:String, nmax = 5)
    else if(DSTaskConfig.jobname.contains("pattern"))
      TextToCT2.kWildcardNgramPatterns_nplus_kplus(_:String, n_max=6, k_max=4)
    else if(DSTaskConfig.jobname.contains("skipgram"))
      TextToCT2.kSkipNgram(_:String, n=3, k=2)
    else
      TextToCT2.ngrams(_:String, n=3)
  }

  /**
    *
    * @param cts a sequence of CT2 objects (not necessarily distinct)
    * @return a sequence of distinct CT2 objects where CT2s are collapsed
    */
  def collapseCT2[T1, T2](cts:TraversableOnce[CT2[T1, T2]]):TraversableOnce[CT2[T1, T2]] = {
    return collapse_foldleft_set_CT2(cts)
  }

  def collapseCT2Min[T1, T2](cts:TraversableOnce[CT2Min[T1, T2]]):TraversableOnce[CT2Min[T1, T2]] = {
    return collapse_foldleft_set_CT2Min(cts)
  }

  def collapse_foldleft[T1, T2](cts:Traversable[CT2[T1, T2]]):TraversableOnce[CT2[T1, T2]] = {
    throw new NotImplementedError()
    //    val partially_collapsed = cts.groupBy(ct => (ct.a, ct.b)).map({case ((a,b), ctlist) => {
    //      ctlist.foreach(ct_x => {
    //        val t:Float = ct_x.n11
    //        val l = ctlist.dropWhile(ct_y => ct_y eq ct_x)
    //        if(!l.isEmpty){
    //          l.tail.foreach(ct_y => {
    //            ct_x.n11 += ct_y.n11;
    //            ct_y.n11 += t
    //          })}
    //      });
    //      ctlist.find(a=>true).get
    //    }})
    //    partially_collapsed

    // group by all four cases and perform adding accordingly
    //    val partially_collapsed = cts.groupBy(ct => (ct.a, ct.b)).map({case ((a,b), ctlist) => {
    //      ctlist.foreach(ct_x => {
    //        val t:Float = ct_x.n11;
    //        ctlist.foreach(ct_y => {
    //          ct_x.n11 += ct_y.n11;
    //          ct_y.n11 += t
    //        })
    //      });
    //      ctlist.find(a=>true).get
    //    }}).groupBy(_.a).flatMap({case ((a), ctlist) => {
    //      ctlist.foreach(ct_x => {
    //        val t:Float = ct_x.n11;
    //        ctlist.foreach(ct_y => {
    //          ct_x.n1dot += ct_y.n11;
    //          ct_y.n1dot += t
    //        })
    //      });
    //      ctlist}
    //    }).groupBy(_.b).flatMap({ case ((b), ctlist) => {
    //      ctlist.foreach(ct_x => {
    //        val t: Float = ct_x.n11;
    //        ctlist.foreach(ct_y => {
    //          ct_x.ndot1 += ct_y.n11;
    //          ct_y.ndot1 += t
    //        })
    //      });
    //      ctlist
    //    }})
    //    partially_collapsed.map(ct_x => {
    //      val t: Float = ct_x.n11;
    //      partially_collapsed.foreach(ct_y => {
    //        ct_x.n += ct_y.n11;
    //        ct_y.n += t
    //      })
    //      ct_x
    //    })

  }

  def collapse_foldleft_set_CT2Min[T1, T2](cts:TraversableOnce[CT2Min[T1, T2]]):TraversableOnce[CT2Min[T1, T2]] = {
    cts.foldLeft(Set[CT2Min[T1, T2]]())((s, x) => {
      val t:CT2Min[T1, T2] = x.copy()
      s.foreach(y => {x += y; y += t })
      s + x
    })
  }

  def collapse_foldleft_set_CT2[T1, T2](cts:TraversableOnce[CT2[T1, T2]]):TraversableOnce[CT2[T1, T2]] = {
    cts.foldLeft(Set[CT2[T1, T2]]())((s, x) => {
      val t:CT2[T1, T2] = x.copy()
      s.foreach(y => {x += y; y += t })
      s + x
    })
  }


  def collapse_explicit_loop_mutable[T1, T2](cts:TraversableOnce[CT2[T1, T2]]):TraversableOnce[CT2[T1, T2]] = {
    val cts_collapsed:mutable.Set[CT2[T1, T2]] = mutable.Set()
    for(ct2_x <- cts){
      val temp:CT2[T1, T2] = ct2_x.copy()
      for(ct2_y <- cts_collapsed){
        ct2_x.+=(ct2_y)
        ct2_y.+=(temp)
      }
      cts_collapsed += ct2_x
    }
    return cts_collapsed
  }

}
