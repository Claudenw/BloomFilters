package org.xenei.bloomfilter;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToLongBiFunction;

import org.junit.Before;
import org.junit.Test;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.hasher.DynamicHasher;

public class CountingBloomFilterTest {

    Shape shape;
    DynamicHasher hasher;

    @Before
    public void setup() throws Exception {
        HasherFactory.register("TestFunc", TestFunc.class);
        hasher = HasherFactory.getHasher("TestFunc");
        hasher.with("Hello");
        shape = new Shape(hasher, 3, 72, 17);
    }

    @Test
    public void ConstructorTest_Hasher() {
        CountingBloomFilter bf = new CountingBloomFilter(hasher, shape);
        LongBuffer lb = bf.getBits();
        assertEquals(0x1FFFF, lb.get(0));
        assertEquals(1, lb.limit());
        assertEquals(17, bf.getCounts().count());
        assertEquals(Integer.valueOf(1), bf.getCounts().map(Map.Entry::getValue).max(Integer::compare).get());
        assertEquals(Integer.valueOf(1), bf.getCounts().map(Map.Entry::getValue).min(Integer::compare).get());
    }

    @Test
    public void ConstructorTest() {
        CountingBloomFilter bf = new CountingBloomFilter(shape);
        LongBuffer lb = bf.getBits();
        assertEquals(0, lb.limit());
        assertEquals(0, bf.getCounts().count());
    }

    @Test
    public void HammingTest() {
        CountingBloomFilter bf = new CountingBloomFilter(hasher, shape);
        assertEquals(17, bf.cardinality());
    }

    @Test
    public void orCardinalityTest() {
        CountingBloomFilter bf = new CountingBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        CountingBloomFilter bf2 = new CountingBloomFilter(hasher2, shape);
        assertEquals(27, bf.orCardinality(bf2));
    }

    @Test
    public void andCardinalityTest() {
        CountingBloomFilter bf = new CountingBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        CountingBloomFilter bf2 = new CountingBloomFilter(hasher2, shape);

        assertEquals(7, bf.andCardinality(bf2));
    }

    @Test
    public void xorCardinalityTest() {
        CountingBloomFilter bf = new CountingBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        CountingBloomFilter bf2 = new CountingBloomFilter(hasher2, shape);

        assertEquals(20, bf.xorCardinality(bf2));
    }

    @Test
    public void mergeTest() {
        CountingBloomFilter bf = new CountingBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        BloomFilter bf2 = new BitSetBloomFilter(hasher2, shape);
        bf.merge(bf2);
        assertEquals(27, bf.hammingValue());

        assertEquals(27, bf.getCounts().count());
        assertEquals(Integer.valueOf(2), bf.getCounts().map(Map.Entry::getValue).max(Integer::compare).get());
        assertEquals(Integer.valueOf(1), bf.getCounts().map(Map.Entry::getValue).min(Integer::compare).get());

        Map<Integer, Integer> m = new HashMap<Integer, Integer>();
        bf.getCounts().forEach(e -> m.put(e.getKey(), e.getValue()));
        assertEquals(2, m.get(10).intValue());
        assertEquals(2, m.get(11).intValue());
        assertEquals(2, m.get(12).intValue());
        assertEquals(2, m.get(13).intValue());
        assertEquals(2, m.get(14).intValue());
        assertEquals(2, m.get(15).intValue());
        assertEquals(2, m.get(16).intValue());

    }

    @Test
    public void mergeTest_Shape_Hasher() {
        CountingBloomFilter bf = new CountingBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        bf.merge(shape, hasher2);
        assertEquals(27, bf.hammingValue());

        assertEquals(27, bf.getCounts().count());
        assertEquals(Integer.valueOf(2), bf.getCounts().map(Map.Entry::getValue).max(Integer::compare).get());
        assertEquals(Integer.valueOf(1), bf.getCounts().map(Map.Entry::getValue).min(Integer::compare).get());

        Map<Integer, Integer> m = new HashMap<Integer, Integer>();
        bf.getCounts().forEach(e -> m.put(e.getKey(), e.getValue()));
        assertEquals(2, m.get(10).intValue());
        assertEquals(2, m.get(11).intValue());
        assertEquals(2, m.get(12).intValue());
        assertEquals(2, m.get(13).intValue());
        assertEquals(2, m.get(14).intValue());
        assertEquals(2, m.get(15).intValue());
        assertEquals(2, m.get(16).intValue());

    }

    public static class TestFunc implements ToLongBiFunction<ByteBuffer, Integer> {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return buffer.equals(ByteBuffer.wrap("Hello".getBytes())) ? seed : seed + 10;
        }

    }
}
