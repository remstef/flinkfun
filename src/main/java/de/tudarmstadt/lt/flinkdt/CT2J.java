package de.tudarmstadt.lt.flinkdt;

import de.tudarmstadt.lt.scalautils.FormatUtils;
import org.apache.commons.lang.StringUtils;
import scala.Tuple6;

/**
 * Created by Steffen Remus
 */
public class CT2J<T1 extends Comparable<?>, T2 extends Comparable<?>> extends CT2MinJ<T1,T2>{

    public CT2J(){
        super();
    }

    public CT2J(T1 A, T2 B, float n11){
        super(A,B,n11);
    }

    public Tuple6<String,String,String,String,String,String> toStringTuple6() {
        return new Tuple6(
                String.format("%s",A),
                String.format("%s",B),
                FormatUtils.format(n11),"","","");
    }

    public String[] toStringArray() {
        return new String[]{
                String.format("%s",A),
                String.format("%s",B),
                FormatUtils.format(n11),"","",""
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
