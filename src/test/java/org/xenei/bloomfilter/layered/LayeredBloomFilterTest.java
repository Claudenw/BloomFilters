package org.xenei.bloomfilter.layered;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.AbstractBloomFilterTest;
import org.apache.commons.collections4.bloomfilter.SparseBloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.TestingHashers;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.DefaultBloomFilterTest;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IncrementingHasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public class LayeredBloomFilterTest extends AbstractBloomFilterTest<LayeredBloomFilter> {

    public LayeredBloomFilter createLayeredFilter(Shape shape, int maxDepth, Predicate<LayerManager> extendCheck) {
        return new LayeredBloomFilter( shape, new LayerManager(
                LayerManager.FilterSupplier.simple(shape),
                extendCheck,
                LayerManager.Cleanup.onMaxSize(maxDepth)
                ) );
    }
    

    @Override
    protected LayeredBloomFilter createEmptyFilter(Shape shape) {
        return createLayeredFilter( shape, 10, LayerManager.ExtendCheck.ADVANCE_ON_POPULATED);
    }

    
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
        LayeredBloomFilter filter = createLayeredFilter(getTestShape(), 10, LayerManager.ExtendCheck.ADVANCE_ON_POPULATED );
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
        LayeredBloomFilter filter = createLayeredFilter(getTestShape(), 10, LayerManager.ExtendCheck.ADVANCE_ON_POPULATED );
        filter.merge( TestingHashers.FROM1);
        filter.merge( TestingHashers.FROM11);
        filter.merge( new IncrementingHasher(11, 2));
        filter.merge( TestingHashers.populateFromHashersFrom1AndFrom11(new SimpleBloomFilter(getTestShape())));
        int[] result = filter.find( TestingHashers.FROM1);
        assertEquals( 2, result.length);
        assertEquals( 0, result[0] );
        assertEquals( 3, result[1] );
        result = filter.find( TestingHashers.FROM11);
        assertEquals( 2, result.length);
        assertEquals( 1, result[0] );
        assertEquals( 3, result[1] );
    }

    /**
     * Tests that the estimated union calculations are correct.
     */
    @Test
    public final void testEstimateUnionCrossTypes() {
        final BloomFilter bf = createFilter(getTestShape(), TestingHashers.FROM1);
        final BloomFilter bf2 = new DefaultBloomFilterTest.SparseDefaultBloomFilter(getTestShape());
        bf2.merge(TestingHashers.FROM11);

        assertEquals(2, bf.estimateUnion(bf2));
        assertEquals(2, bf2.estimateUnion(bf));
    }
}
