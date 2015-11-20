package de.tudarmstadt.lt.flinkdt

/**
  * Created by sr on 11/20/15.
  */
class PatGen {

  val WILDCARD = "*"
  val REMOVE_LEADING_WILDCARDS = true
  val REMOVE_TRAILING_WILDCARDS = true

  def comb(n:Int, k:Int, fixed:Seq[Int]) = Range(0,n).combinations(k).filter(_.intersect(fixed).isEmpty);

  def raw_patterns() = {
    val index_combinations = comb

  }

//  public List<int[]> scalacomb(int k, int n, int[] fixed){
//    //
//    // Range(0,n).combinations(k).filter(_.intersect(fixed).isEmpty).foreach(println)
//    //
//    List<int[]> s = new ArrayList<>();
//    Range r = new Range(0,n,1);
//    Iterator<Seq<Object>> comb = r.combinations(k);
//    outer: while(comb.hasNext()){
//      Seq<Object> current = comb.next();
//      Iterator<Object> i = current.iterator();
//      int[] intcomb = new int[k];
//      int c = 0;
//      while(i.hasNext()){
//        Integer l = (Integer)i.next();
//        if(ArrayUtils.contains(fixed, l))
//          continue outer;
//        intcomb[c++] = l;
//      }
//      s.add(intcomb);
//    }
//    if(SORT_OUTPUT)
//      Collections.sort(s, _comb_comparator);
//    return s;
//  }


}
