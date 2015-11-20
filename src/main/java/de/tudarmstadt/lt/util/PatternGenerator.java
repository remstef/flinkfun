/*
 *   Copyright 2015
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.tudarmstadt.lt.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.Range;

/**
 *
 * @author Steffen Remus
 **/
public class PatternGenerator<O> {

    public static PatternGenerator<String> STRING_GENERATOR =  new PatternGenerator<>("*");

    private static int[] EMPTY_ARR = new int[0];

    private static Comparator<int[]> _comb_comparator = new Comparator<int[]>(){
        @Override
        public int compare(int[] o1, int[] o2) {
            for(int i = 0; i < o1.length; i++){
                int r = o2[i] - o1[i];
                if(r != 0)
                    return r;
            }
            return 0;
        }
    };

    public static String[] get_patterns(String str){
        List<String> words = Arrays.asList(str.split("\\s+"));
        List<String> patterns = new ArrayList<String>();
        for(int i = 1; i < words.size(); i++){
            for(List<String> pattern : STRING_GENERATOR.get_merged_patterns(words, i))
                patterns.add(StringUtils.join(pattern, " "));
        }
        return patterns.toArray(new String[0]);
    }

    public PatternGenerator(){
        this((O)new Object(){
            @Override
            public String toString() {
                return "*";
            }
        });
    }

    public PatternGenerator(O wildcard){
        WILDCARD = wildcard;
    }

    public O WILDCARD;

    public boolean SORT_OUTPUT = false;

    public boolean REMOVE_LEADING_WILDCARDS = false;

    public boolean REMOVE_TRAILING_WILDCARDS = false;

    public List<List<O>> get_merged_patterns(List<O> words, int num_wildcards){
        return get_merged_patterns(words, num_wildcards, EMPTY_ARR);
    }

    public List<List<O>> get_merged_patterns(List<O> words, int num_wildcards, int[] fixed){
        List<List<O>> raw_patterns = get_raw_patterns(words, num_wildcards, fixed);
        List<List<O>> merged_patterns = merge_wildcards(raw_patterns);
        return merged_patterns;

    }

    public List<List<O>> merge_wildcards(List<List<O>> patterns){
        List<List<O>> merged_patterns = new ArrayList<List<O>>(patterns.size());
        for(int i = 0; i < patterns.size(); i++){
            merged_patterns.add(merge_wildcards_pattern(patterns.get(i)));
        }
        return merged_patterns;
    }

    public List<O> merge_wildcards_pattern(List<O> pattern){
        List<O> merged_pattern = new ArrayList<O>(pattern.size());
        int first = 0;
        while(REMOVE_LEADING_WILDCARDS && WILDCARD.equals(pattern.get(first)))
            first++;
        int last = pattern.size() - 1;
        while(REMOVE_TRAILING_WILDCARDS && WILDCARD.equals(pattern.get(last)))
            last--;
        merged_pattern.add(pattern.get(first));
        for(int i = first + 1; i <= last; i++){
            if(WILDCARD.equals(pattern.get(i)) && WILDCARD.equals(pattern.get(i-1)))
                continue;
            merged_pattern.add(pattern.get(i));
        }
        return merged_pattern;
    }

    public List<List<O>> get_raw_patterns(List<O> words, int num_wildcards, int[] fixed){
        List<int[]> combinations = comb(num_wildcards, words.size(), fixed);
        List<List<O>> patterns = new ArrayList<List<O>>(combinations.size());
        for(int c = 0; c < combinations.size(); c++){
            int[] comb = combinations.get(c);
            List<O> pattern = new ArrayList<O>(words);
            for(int i = 0; i < comb.length; i++){
                pattern.set(comb[i], WILDCARD);
            }
            patterns.add(pattern);
        }
        return patterns;
    }

    private static int[] bitadd(int u, int k){
        int[] r_ = new int[k];
        for(int n = 0, i = 0;u > 0;++n, u>>= 1)
            if((u & 1) > 0)
                r_[i++]= n;
        return r_;
    }

    private static int bitcount(int u){
        int n;
        for(n= 0;u > 0;++n, u&= (u - 1));//Turn the last set bit to a 0
        return n;
    }

    public List<int[]> comb(int k, int n, int[] fixed){
        List<int[]> s= new ArrayList<int[]>();
        for(int u= 0;u < 1 << n;u++)
            if(bitcount(u) == k){
                int[] c = bitadd(u, k);
                boolean add = true;
                for(int f : fixed)
                    add &= !ArrayUtils.contains(c, f);
                if(add)
                    s.add(c);
            }
        if(SORT_OUTPUT)
            Collections.sort(s, _comb_comparator);
        return s;
    }

    public List<int[]> scalacomb(int k, int n, int[] fixed){
        //
        // Range(0,n).combinations(k).filter(_.intersect(fixed).isEmpty).foreach(println)
        //
        List<int[]> s = new ArrayList<>();
        Range r = new Range(0,n,1);
        Iterator<Seq<Object>> comb = r.combinations(k);
        outer: while(comb.hasNext()){
            Seq<Object> current = comb.next();
            Iterator<Object> i = current.iterator();
            int[] intcomb = new int[k];
            int c = 0;
            while(i.hasNext()){
                Integer l = (Integer)i.next();
                if(ArrayUtils.contains(fixed, l))
                    continue outer;
                intcomb[c++] = l;
            }
            s.add(intcomb);
        }
        if(SORT_OUTPUT)
            Collections.sort(s, _comb_comparator);
        return s;
    }



    public static void main(String[] args) {
//        for(int[] r : index_combinations(4, 2)){
//            System.out.println(Arrays.toString(r));
//        }

        for(List<String> pattern : STRING_GENERATOR.get_merged_patterns(Arrays.asList("The quick brown fox.".split("\\s+")), 2, new int[]{0})) {
            System.out.println(pattern);
        }
    }

}
