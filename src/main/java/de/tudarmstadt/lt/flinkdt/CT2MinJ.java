package de.tudarmstadt.lt.flinkdt;

import de.tudarmstadt.lt.scalautils.FormatUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import scala.Tuple3;
import java.io.Serializable;

/**
 * Created by Steffen Remus
 */
public class CT2MinJ<T1 extends Comparable<?>, T2 extends Comparable<?>> implements Serializable, Comparable<CT2MinJ<T1,T2>>{

    public Comparable<?> A;
    public Comparable<?> B;
    public float n11;

    public CT2MinJ(){}

    public CT2MinJ(T1 A, T2 B, float n11){
        this.A = A;
        this.B = B;
        this.n11 = n11;
    }

    public CT2MinJ<T1, T2> add(CT2MinJ<T1, T2> other) {
//        val newct:this.type = copy().asInstanceOf[this.type]
//        if(A == other.A && B == other.B)
//            newct.n11 += other.n11
//        return newct
        return null;
    }

    public Tuple3<String,String,String> toStringTuple3() {
        return new Tuple3(
                String.format("%s",A),
                String.format("%s",B),
                FormatUtils.format(n11));
    }

    public CT2MinJ<T1,T2> copy(){
        return null;
    }

    public String[] toStringArray() {
        return new String[]{
                String.format("%s",A),
                String.format("%s",B),
                FormatUtils.format(n11)
        };
    }

    @Override
    public int compareTo(CT2MinJ<T1, T2> o) {
        throw new NotImplementedException();
    }

    @Override
    public String toString(){
      return StringUtils.join(toStringArray(),"\t");
    }

    @Override
    public boolean equals(Object that) {
        if(that instanceof CT2MinJ) {
            CT2MinJ ct2 = (CT2MinJ)that;
            if(this.A == null && this.B == null)
                return true;
            return  ((this.A != null && this.A == ct2.A) && (this.B != null && this.B == ct2.B) && (this.n11 == ct2.n11));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 41 * (41 * (41 + (null == A ? 0 : A.hashCode())) + (null == B ? 0 :  B.hashCode())) +  Float.floatToIntBits(this.n11);
    }


}
