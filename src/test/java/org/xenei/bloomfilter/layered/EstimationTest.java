package org.xenei.bloomfilter.layered;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;


import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;

public class EstimationTest {

    static final List<Hasher> hashers = new ArrayList<>();
    static final int limit = 1000;
    static final Shape shape = Shape.fromNP(limit, 0.5 / limit);
    BloomFilter standard;
    LayeredBloomFilter layered10;
    LayeredBloomFilter layered100;
    LayeredBloomFilter layered10S;
    LayeredBloomFilter layered100S;
    List<Integer> standardV = new ArrayList<>();
    List<Integer> layered10V = new ArrayList<>();
    List<Integer> layered10SV = new ArrayList<>();
    List<Integer> layered100V = new ArrayList<>();
    List<Integer> layered100SV = new ArrayList<>();

    Random r = new Random();

    public static void main(String[] args) {
        new EstimationTest().run();
    }

    public void reset() {
        hashers.clear();
        standard = new SimpleBloomFilter(shape);
        layered10 = new LayeredBloomFilter(Shape.fromNP(10, 0.5 / 10),
                new LayerManager(LayerManager.FilterSupplier.simple(shape), 
                        LayerManager.ExtendCheck.advanceOnCount(10),
                        LayerManager.Cleanup.onMaxSize(limit / 10)));

        layered100 = new LayeredBloomFilter(Shape.fromNP(100, 0.5 / 100),
                new LayerManager(LayerManager.FilterSupplier.simple(shape), LayerManager.ExtendCheck.advanceOnCount(100),
                        LayerManager.Cleanup.onMaxSize(limit / 100)));
        layered10S = new LayeredBloomFilter(shape, new LayerManager(LayerManager.FilterSupplier.simple(shape),
                LayerManager.ExtendCheck.advanceOnCount(10), LayerManager.Cleanup.onMaxSize(limit / 10)));
        layered100S = new LayeredBloomFilter(shape, new LayerManager(LayerManager.FilterSupplier.simple(shape),
                LayerManager.ExtendCheck.advanceOnCount(100), LayerManager.Cleanup.onMaxSize(limit / 100)));
        for (int i = 0; i < limit; i++) {
            hashers.add(new EnhancedDoubleHasher(r.nextLong(), r.nextLong()));
        }
    }

    public EstimationTest() {
    }

    private void run() {
        for (int j = 0; j < 10000; j++) {
            reset();
            for (int i = 0; i < limit; i++) {
                if (i > 0 && i % 100 == 0) {
                    calc(i);
                }
                standard.merge(hashers.get(i));
                layered10.merge(hashers.get(i));
                layered10S.merge(hashers.get(i));
                layered100.merge(hashers.get(i));
                layered100S.merge(hashers.get(i));
            }
            calc(limit);
        }
        System.out.printf("counts\tstd\t\t\t10\t\t\t10S\t\t\t100\t\t\t100S\n");
        System.out.format("%s\t%s\t%s\t%s\t%s\t%s\n", "SD", sd(standardV), sd(layered10V), sd(layered10SV),
                sd(layered100V), sd(layered100SV));
    }

    private double sd(List<Integer> lst) {
        double sum[] = { 0 };
        lst.forEach(x -> sum[0] += Math.pow(x, 2));
        return Math.sqrt(sum[0] / lst.size());
    }

    private void calc(int count) {
        standardV.add(count - standard.estimateN());
        layered10V.add(count - layered10.estimateN());
        layered10SV.add(count - layered10S.estimateN());
        layered100V.add(count - layered100.estimateN());
        layered100SV.add(count - layered100S.estimateN());
    }
}