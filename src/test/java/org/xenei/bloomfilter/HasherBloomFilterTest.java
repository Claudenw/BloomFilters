package org.xenei.bloomfilter;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.function.ToLongBiFunction;

import org.junit.Before;
import org.junit.Test;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.hasher.DynamicHasher;

public class HasherBloomFilterTest {

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

        HasherBloomFilter bf = new HasherBloomFilter(hasher, shape);
        LongBuffer lb = bf.getBits();
        assertEquals(0x1FFFF, lb.get(0));
        assertEquals(1, lb.limit());
    }

    @Test
    public void ConstructorTest() {
        HasherBloomFilter bf = new HasherBloomFilter(shape);
        LongBuffer lb = bf.getBits();
        assertEquals(0, lb.limit());
    }

    @Test
    public void HammingTest() {
        HasherBloomFilter bf = new HasherBloomFilter(hasher, shape);
        assertEquals(17, bf.cardinality());
    }

    @Test
    public void orCardinalityTest() {
        HasherBloomFilter bf = new HasherBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter(hasher2, shape);

        assertEquals(27, bf.orCardinality(bf2));
    }

    @Test
    public void andCardinalityTest() {
        HasherBloomFilter bf = new HasherBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter(hasher2, shape);

        assertEquals(7, bf.andCardinality(bf2));
    }

    @Test
    public void xorCardinalityTest() {
        HasherBloomFilter bf = new HasherBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter(hasher2, shape);

        assertEquals(20, bf.xorCardinality(bf2));
    }

    @Test
    public void mergeTest() {
        HasherBloomFilter bf = new HasherBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter(hasher2, shape);
        bf.merge(bf2);
        assertEquals(27, bf.hammingValue());
    }

    @Test
    public void mergeTest_Shape_Hasher() {
        HasherBloomFilter bf = new HasherBloomFilter(hasher, shape);

        DynamicHasher hasher2 = HasherFactory.getHasher("TestFunc");
        hasher2.with("World");

        bf.merge(shape, hasher2 );
        assertEquals(27, bf.hammingValue());
    }

    public static class TestFunc implements ToLongBiFunction<ByteBuffer, Integer> {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return buffer.equals(ByteBuffer.wrap("Hello".getBytes())) ? seed : seed + 10;
        }

    }
}
