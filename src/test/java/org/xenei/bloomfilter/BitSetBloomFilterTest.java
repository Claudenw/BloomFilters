package org.xenei.bloomfilter;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.junit.Before;
import org.junit.Test;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.Hasher.Func;
import org.xenei.bloomfilter.HasherTest.TestFunc;

public class BitSetBloomFilterTest {

    Shape shape;
    Hasher hasher;

    @Before
    public void setup() throws Exception {
        Hasher.register( "TestFunc", TestFunc.class );
        hasher = Hasher.getHasher("TestFunc");
        hasher.with( "Hello");
        shape = new Shape( hasher, 3, 72, 17);
    }

    @Test
    public void ConstructorTest_Hasher() {
        BitSetBloomFilter bf = new BitSetBloomFilter( hasher, shape );
        LongBuffer lb = bf.getBits();
        assertEquals( 0x1FFFF, lb.get(0) );
        assertEquals( 1, lb.limit());
    }

    @Test
    public void ConstructorTest() {
        BitSetBloomFilter bf = new BitSetBloomFilter( shape );
        LongBuffer lb = bf.getBits();
        assertEquals( 0, lb.limit());
    }

    @Test
    public void HammingTest() {
        BitSetBloomFilter bf = new BitSetBloomFilter( hasher, shape );
        assertEquals( 17, bf.cardinality() );
    }

    @Test
    public void orCardinalityTest() {
        BitSetBloomFilter bf = new BitSetBloomFilter( hasher, shape );

        Hasher hasher2 = Hasher.getHasher("TestFunc");
        hasher2.with( "World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter( hasher2, shape );

        assertEquals( 27, bf.orCardinality(bf2) );
    }

    @Test
    public void andCardinalityTest() {
        BitSetBloomFilter bf = new BitSetBloomFilter( hasher, shape );

        Hasher hasher2 = Hasher.getHasher("TestFunc");
        hasher2.with( "World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter( hasher2, shape );

        assertEquals( 7, bf.andCardinality(bf2) );
    }

    @Test
    public void xorCardinalityTest() {
        BitSetBloomFilter bf = new BitSetBloomFilter( hasher, shape );

        Hasher hasher2 = Hasher.getHasher("TestFunc");
        hasher2.with( "World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter( hasher2, shape );

        assertEquals( 20, bf.xorCardinality(bf2) );
    }

    @Test
    public void mergeTest() {
        BitSetBloomFilter bf = new BitSetBloomFilter( hasher, shape );

        Hasher hasher2 = Hasher.getHasher("TestFunc");
        hasher2.with( "World");

        BitSetBloomFilter bf2 = new BitSetBloomFilter( hasher2, shape );
        bf.merge( bf2 );
        assertEquals( 27, bf.hammingValue());
    }

    public static class TestFunc implements Func {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return buffer.equals( ByteBuffer.wrap( "Hello".getBytes()))?seed:seed+10;
        }

    }
}
