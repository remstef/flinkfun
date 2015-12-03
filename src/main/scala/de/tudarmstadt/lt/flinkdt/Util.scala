package de.tudarmstadt.lt.flinkdt

import scala.collection.mutable

/**
  * Created by Steffen Remus
  */
object Util {

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
//    val partially_collapsed = cts.groupBy(ct => (ct.A, ct.B)).map({case ((a,b), ctlist) => {
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
    //    val partially_collapsed = cts.groupBy(ct => (ct.A, ct.B)).map({case ((a,b), ctlist) => {
    //      ctlist.foreach(ct_x => {
    //        val t:Float = ct_x.n11;
    //        ctlist.foreach(ct_y => {
    //          ct_x.n11 += ct_y.n11;
    //          ct_y.n11 += t
    //        })
    //      });
    //      ctlist.find(a=>true).get
    //    }}).groupBy(_.A).flatMap({case ((a), ctlist) => {
    //      ctlist.foreach(ct_x => {
    //        val t:Float = ct_x.n11;
    //        ctlist.foreach(ct_y => {
    //          ct_x.n1dot += ct_y.n11;
    //          ct_y.n1dot += t
    //        })
    //      });
    //      ctlist}
    //    }).groupBy(_.B).flatMap({ case ((b), ctlist) => {
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
      val t:CT2[T1, T2] = x.copyDeep()
      s.foreach(y => {x += y; y += t })
      s + x
    })
  }


  def collapse_explicit_loop_mutable[T1, T2](cts:TraversableOnce[CT2[T1, T2]]):TraversableOnce[CT2[T1, T2]] = {
    val cts_collapsed:mutable.Set[CT2[T1, T2]] = mutable.Set()
    for(ct2_x <- cts){
      val temp:CT2[T1, T2] = ct2_x.copyDeep()
      for(ct2_y <- cts_collapsed){
        ct2_x.+=(ct2_y)
        ct2_y.+=(temp)
      }
      cts_collapsed += ct2_x
    }
    return cts_collapsed
  }

}
