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
//    return collapse_explicit_loop(cts, add_ndocs)
    return collapse_foldleft_set(cts)
  }

//  def collapse_foldleft_list[T](cts:TraversableOnce[CT2[T]], add_ndocs:Boolean):TraversableOnce[CT2[T]] = {
//    cts.foldLeft(List[CT2[T]]())((l, x) => {
//      val t:CT2[T] = x.copy()
//      l.foreach(y => {x.+=(y, add_ndocs); y.+=(t, add_ndocs)})
//      l :+ x
//    })
//  }

  def collapse_foldleft_set[T](cts:TraversableOnce[CT2[T]]):TraversableOnce[CT2[T]] = {
    cts.foldLeft(Set[CT2[T]]())((s, x) => {
      val t:CT2[T] = x.copy()
      s.foreach(y => {x += y; y += t })
      s + x
    })
  }


  def collapse_explicit_loop[T](cts:TraversableOnce[CT2[T]]):TraversableOnce[CT2[T]] = {
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
