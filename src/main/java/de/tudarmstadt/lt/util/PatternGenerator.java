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

/**
 *
 * @author Steffen Remus
 **/
public class PatternGenerator<O> {

    private static int[] EMPTY_ARR = new int[0];

    public String WILDCARD = "*";

    public boolean SORT_OUTPUT = false;

    public boolean REMOVE_LEADING_WILDCARDS = false;

    public boolean REMOVE_TRAILING_WILDCARDS = false;

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

    public String[] get_patterns(String str){
        List<String> words = Arrays.asList(str.split("\\s+"));
        List<String> patterns = new ArrayList<String>();
        for(int i = 1; i < words.size(); i++){
            for(List<String> pattern : get_merged_patterns(words, i))
                patterns.add(StringUtils.join(pattern, " "));
        }
        return patterns.toArray(new String[0]);
    }

    public List<List<String>> get_merged_patterns(List<String> words, int num_wildcards){
        return get_merged_patterns(words, num_wildcards, EMPTY_ARR);
    }

    public List<List<String>> get_merged_patterns(List<String> words, int num_wildcards, int[] fixed){
        List<List<String>> raw_patterns = get_raw_patterns(words, num_wildcards, fixed);
        List<List<String>> merged_patterns = merge_wildcards(raw_patterns);
        return merged_patterns;

    }

    public List<List<String>> merge_wildcards(List<List<String>> patterns){
        List<List<String>> merged_patterns = new ArrayList<List<String>>(patterns.size());
        for(int i = 0; i < patterns.size(); i++){
            merged_patterns.add(merge_wildcards_pattern(patterns.get(i)));
        }
        return merged_patterns;
    }

    public List<String> merge_wildcards_pattern(List<String> pattern){
        List<String> merged_pattern = new ArrayList<String>(pattern.size());
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

    public List<List<String>> get_raw_patterns(List<String> words, int num_wildcards, int[] fixed){
        List<int[]> combinations = comb(num_wildcards, words.size(), fixed);
        List<List<String>> patterns = new ArrayList<List<String>>(combinations.size());
        for(int c = 0; c < combinations.size(); c++){
            int[] comb = combinations.get(c);
            List<String> pattern = new ArrayList<String>(words);
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

    public static List<int[]> index_combinations(int max_index, int n_indices){
        List<int[]> result = new ArrayList<int[]>();
        index_combinations(max_index, n_indices, new int[n_indices], 0, result);
        return result;
    }

    private static void index_combinations(int max_index, int n_indices, int[] current_combination, int current_combination_index, List<int[]> result) {
        if(current_combination_index == n_indices) {
            result.add(current_combination);
        } else {
            for(int i = 0; i < max_index; i++) {
                int[] old_combination = ArrayUtils.clone(current_combination);
                int old_combination_index = current_combination_index;
                current_combination[current_combination_index++] = i;
                index_combinations(max_index, n_indices, current_combination, current_combination_index, result);
                current_combination = old_combination;
                current_combination_index = old_combination_index;
            }
        }
    }


    public static void main(String[] args) {
//        for(int[] r : index_combinations(4, 2)){
//            System.out.println(Arrays.toString(r));
//        }

        for(List<String> pattern : new PatternGenerator<String>().get_merged_patterns(Arrays.asList("The quick brown fox.".split("\\s+")), 2, new int[]{0})) {
            System.out.println(pattern);
        }
    }

}
