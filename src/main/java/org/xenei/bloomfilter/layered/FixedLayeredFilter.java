package org.xenei.bloomfilter.layered;

import java.util.List;
import java.util.Stack;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.SparseBloomFilter;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.SetOperations;
import org.apache.commons.collections4.bloomfilter.Shape;

/**
 * A simple layered filter with a specified max depth.
 * If the depth is exceeded the first filter added is removed and all subsequent filters are moved one step
 * closer to the start.
 */
public class FixedLayeredFilter extends LayeredBloomFilter {
    private final BloomFilter[] filters;
    private int currentIdx;
    
    public FixedLayeredFilter(final Shape shape, int maxDepth) {
        this( shape, maxDepth, (x)-> false );
    }
    
    public FixedLayeredFilter(final Shape shape, int maxDepth, Predicate<LayeredBloomFilter> extendCheck) {
        super(shape, maxDepth, extendCheck);
        this.filters = new BloomFilter[maxDepth];
        currentIdx = -1;
        next();
    }
    
    @Override
    public BloomFilter target() {
        return filters[currentIdx];
    }

    @Override
    public FixedLayeredFilter copy() {
        FixedLayeredFilter result = new FixedLayeredFilter(shape, maxDepth, extendCheck);
        System.arraycopy(filters, 0, result.filters, 0, currentIdx);
        result.currentIdx = currentIdx;
        return result;
    }

    @Override
    public void clear() {
        Arrays.setAll(filters, null);
        currentIdx = -1;
        next();
    }
       
    @Override
    public int cardinality() {
        return SetOperations.cardinality(this);
    }

    @Override
    public int getDepth() {
        return currentIdx+1;
    }

    @Override
    public void clear(int level) {
        filters[level] = new SimpleBloomFilter(shape);
    }

    @Override
    public boolean forEachBloomFilter(Predicate<BloomFilter> bloomFilterPredicate) {
        for (int i=0;i<currentIdx;i++) {
            if (!bloomFilterPredicate.test(filters[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void next() {
        filters[++currentIdx] = new SimpleBloomFilter(shape);
    }

}
