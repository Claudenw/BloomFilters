package org.xenei.bloomfilter.layered;

import java.util.List;
import java.util.Stack;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
public class SimpleLayeredFilter extends LayeredBloomFilter {
    private final Deque<BloomFilter> filters;
    
    public SimpleLayeredFilter(final Shape shape, int maxDepth) {
        this( shape, maxDepth, (x)-> false );
    }
    
    public SimpleLayeredFilter(final Shape shape, int maxDepth, Predicate<LayeredBloomFilter> extendCheck) {
        super(shape, maxDepth, extendCheck);
        this.filters = new ArrayDeque<BloomFilter>(maxDepth);
        next();
    }
    @Override
    public BloomFilter target() {
        return filters.peekLast();
    }

    @Override
    public SimpleLayeredFilter copy() {
        SimpleLayeredFilter result = new SimpleLayeredFilter(shape, maxDepth, extendCheck);
        result.filters.clear();
        forEachBloomFilter( x -> result.filters.add(x.copy()));
        return result;
    }

    @Override
    public void clear() {
        this.filters.clear();
        next();
    }
       
    @Override
    public int cardinality() {
        return SetOperations.cardinality(this);
    }

    @Override
    public int getDepth() {
        return filters.size();
    }

    @Override
    public void clear(int level) {
        filters.remove(level);
    }

    @Override
    public boolean forEachBloomFilter(Predicate<BloomFilter> bloomFilterPredicate) {
        for (BloomFilter bf : filters) {
            if (!bloomFilterPredicate.test(bf)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void next() {
        if (getDepth() == maxDepth) {
            filters.removeFirst();
        }
        filters.add(new SimpleBloomFilter(shape)); 
    }

}
