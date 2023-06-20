package org.xenei.bloomfilter.layered;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.AbstractBloomFilterTest;
import org.apache.commons.collections4.bloomfilter.SparseBloomFilter;
import org.apache.commons.collections4.bloomfilter.TestingHashers;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IncrementingHasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public abstract class AbstractLayeredFilterTest<T extends LayeredBloomFilter> extends AbstractBloomFilterTest<T> {

    abstract T createLayeredFilter(Shape shape, int maxDepth, Predicate<LayeredBloomFilter> extendCheck);
    
    protected BloomFilter makeFilter(int ... values) {
        return makeFilter(IndexProducer.fromIndexArray(values));
    }
    
    protected BloomFilter makeFilter(IndexProducer p) {
        BloomFilter bf = new SparseBloomFilter(getTestShape());
        bf.merge(p);
        return bf;
    }
    
    protected BloomFilter makeFilter(Hasher h) {
        BloomFilter bf = new SparseBloomFilter(getTestShape());
        bf.merge(h);
        return bf;
    }
    @Test
    public void testMultipleFilters() {
        T filter = createLayeredFilter(getTestShape(), 10, LayeredBloomFilter.ADVANCE_ON_POPULATED );
        filter.merge( TestingHashers.FROM1);
        filter.merge( TestingHashers.FROM11);
        assertEquals(2,filter.getDepth());
        assertTrue( filter.contains( makeFilter(TestingHashers.FROM1)));
        assertTrue( filter.contains( makeFilter(TestingHashers.FROM11)));
        BloomFilter t1 = makeFilter(6,7,17,18,19);
        assertFalse( filter.contains(t1));
        assertFalse( filter.copy().contains(t1));
        assertTrue( filter.flatten().contains(t1));
    }
    
    @Test
    public void testFind() {
        T filter = createLayeredFilter(getTestShape(), 10, LayeredBloomFilter.ADVANCE_ON_POPULATED );
        filter.merge( TestingHashers.FROM1);
        filter.merge( TestingHashers.FROM11);
        filter.merge( new IncrementingHasher(11, 2));
        filter.merge( TestingHashers.populateFromHashersFrom1AndFrom11(filter));
        int[] result = filter.find( TestingHashers.FROM1);
        assertEquals( 0, result[0] );
        assertEquals( 3, result[1] );
        result = filter.find( TestingHashers.FROM11);
        assertEquals( 1, result[0] );
        assertEquals( 3, result[1] );
    }
   
    
}
