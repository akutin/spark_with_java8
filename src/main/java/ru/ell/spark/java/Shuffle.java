package ru.ell.spark.java;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Alexey Kutin on 11/17/2016.
 */
public class Shuffle {

    public static void main(String[] args) throws Exception {
        final List<Integer> map = new ArrayList<>(128);
        for(int i = 0; i<128; i++) {
            map.add(i);
        }
        Collections.shuffle( map);
        System.out.println(map);

        final List<IdMap> output = new ArrayList<>(100);
        for(long l = 1; l <100; l++) {
            final String binary = String.format("%128s", Long.toBinaryString(l)).replace(' ', '0');
            final StringBuilder out = new StringBuilder();
            for(int i = 0; i<binary.length(); i++) {
                out.append(binary.charAt(map.get(i)));
            }
            output.add(new IdMap(l, new BigInteger(out.toString(), 2)));
        }

        Collections.sort(output);
        for(final IdMap id : output) {
            System.out.println( id);
        }
    }

    static class IdMap implements Comparable<IdMap> {
        private final long id;
        private final BigInteger out;

        IdMap(long id, BigInteger out) {
            this.id = id;
            this.out = out;
        }

        @Override
        public int compareTo(IdMap o) {
            return out.compareTo(o.out);
        }

        @Override
        public String toString() {
            return "IdMap{" +
                    "id=" + id +
                    ", out=" + out +
                    '}';
        }
    }
}
