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
  def collapse[T](cts:TraversableOnce[CT2[T]]):TraversableOnce[CT2[T]] = {
    return collapse_foldleft_set(cts)
//    return collapse_foldleft_set(cts)
  }

  def collapse_foldleft[T](cts:Traversable[CT2[T]]):TraversableOnce[CT2[T]] = {
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

  def collapse_foldleft_set[T](cts:TraversableOnce[CT2[T]]):TraversableOnce[CT2[T]] = {
    cts.foldLeft(Set[CT2[T]]())((s, x) => {
      val t:CT2[T] = x.copy()
      s.foreach(y => {x += y; y += t })
      s + x
    })
  }


  def collapse_explicit_loop_mutable[T](cts:TraversableOnce[CT2[T]]):TraversableOnce[CT2[T]] = {
    val cts_collapsed:mutable.Set[CT2[T]] = mutable.Set()
    for(ct2_x <- cts){
      val temp:CT2[T] = ct2_x.copy()
      for(ct2_y <- cts_collapsed){
        ct2_x.+=(ct2_y)
        ct2_y.+=(temp)
      }
      cts_collapsed += ct2_x
    }
    return cts_collapsed
  }

}
