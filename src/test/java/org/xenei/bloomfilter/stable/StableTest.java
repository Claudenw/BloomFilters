package org.xenei.bloomfilter.stable;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;

public class StableTest {
    
    private static Random random = new Random();
    
    public static void main(String[] args) {
        int limit=10000;
        double prob = 1.0/1000;
        StableShape shape = StableShape.builder(Shape.fromNP(2000, prob)).setMax(3).setFps(prob).build();
        StableBloomFilter sbf = new StableBloomFilter( shape );
        List<HasherRec> hashers = new ArrayList<>();
        int counts[] = new int[limit];
        int cardinality[] = new int[limit];
        for (int i=0;i<limit;i++) {
            HasherRec rec = new HasherRec(i);
            hashers.add(rec);
            sbf.merge( rec.hasher);
            for (int j=0;j<=i;j++) {
                rec = hashers.get(j);
                if (sbf.contains(rec.hasher)) {
                    counts[i]++;
                }
                if (!rec.wasRemoved() && !sbf.contains( rec.hasher )) {
                    rec.removed = i;
                }
            }
            cardinality[i] = sbf.cardinality();
        }
        
        int removed = 0;
        double avg = 0;
        for (HasherRec hr : hashers) {
            if (hr.wasRemoved()) {
                removed++;
                avg += hr.elapsed();
            }
        }
        avg = avg/removed;
        double var = 0;
        long present = 0;
        for (HasherRec hr : hashers) {
            if (hr.wasRemoved()) {
                var += Math.pow(avg-hr.elapsed(),2);
            }
            if (sbf.contains( hr.hasher )) {
                present ++;
            }
        }
        var /= removed;
        double sd = Math.sqrt(var);
        System.out.format( "Removed\n n: %s\n avg: %s\n sd %s\n var %s\npresent %s\n", removed, avg, sd, var, present);
        
        stats( "Limit", counts );
        stats( "Cardinality", cardinality);
         
        for (int i=100;i<limit;i+=1000)
            System.out.format( "%s %s %s\n", i, counts[i], shape.getShape().estimateN( cardinality[i] ));

        System.out.println( "\nShape: "+shape );
        
        double estimatedN =  shape.getShape().estimateN( shape.expectedCardinality );
        System.out.format( "~N %s\n", estimatedN);
    }
    
    private static void stats(String label, int[] observed) {
        final double avg = Arrays.stream(observed).mapToDouble(x -> (double)x).sum()/observed.length;
        double var = Arrays.stream(observed).mapToDouble( x -> Math.pow(avg-x, 2)).sum()/ observed.length;;
        double sd = Math.sqrt(var);
        System.out.format( "%s\n n: %s\n avg: %s\n sd %s\n var %s\n\n", label, observed.length, avg, sd, var);
    }
    
    private static class HasherRec {
        final Hasher hasher;
        final int id;
        int removed = 0;
        
        HasherRec(int id) {
            hasher = new EnhancedDoubleHasher( random.nextLong(), random.nextLong());
            this.id = id;
        }
        
        boolean wasRemoved() {
            return removed != 0;
        }
        
        int elapsed() {
            return removed-id;
        }
        
    }
}
