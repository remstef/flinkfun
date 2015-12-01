package de.tudarmstadt.lt.flinkdt;

import de.tudarmstadt.lt.scalautils.FormatUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.CopyableValue;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by sr on 12/1/15.
 */
public class CT2MinJ<T1 extends Comparable<?>, T2 extends Comparable<?>> {

    public Comparable<?> A;
    public Comparable<?> B;
    public float n11;

    public CT2MinJ(){}

    public CT2MinJ(T1 A, T2 B, float n11){
        this.A = A;
        this.B = B;
        this.n11 = n11;
    }

    public Tuple3<String,String,String> toStringTuple() {
        return Tuple3.of(
                String.format("%s",A),
                String.format("%s",B),
                FormatUtils.format(n11));
    }

    public String[] toStringArray() {
        return new String[]{
                String.format("%s",A),
                String.format("%s",B),
                FormatUtils.format(n11)
        };
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
