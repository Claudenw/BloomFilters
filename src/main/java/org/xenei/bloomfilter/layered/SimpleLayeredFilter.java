package org.xenei.bloomfilter.layered;

import java.util.List;
import java.util.Stack;
import java.util.ArrayList;
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

public class SimpleLayeredFilter extends LayeredBloomFilter {
    private final List<BloomFilter> filters;
    
    public SimpleLayeredFilter(final Shape shape, int maxDepth) {
        this( shape, maxDepth, (x)-> false );
    }
    
    public SimpleLayeredFilter(final Shape shape, int maxDepth, Predicate<LayeredBloomFilter> extendCheck) {
        super(shape, maxDepth, extendCheck);
        this.filters = new ArrayList<BloomFilter>();
        next();
    }
    @Override
    public BloomFilter target() {
        return filters.get(filters.size()-1);
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
    protected boolean doContains(BloomFilter bf) {
        if (bf instanceof LayeredBloomFilter) {
            boolean[] result = {false};
            // return false when we have found a match.
            ((LayeredBloomFilter)bf).forEachBloomFilter( x -> {result[0] |= doContains(x); return result[0];});
            return result[0];
        }
        return filters.stream().anyMatch( x -> { 
            return x.contains(bf);});
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
            filters.remove(0);
        }
        filters.add(new SimpleBloomFilter(shape)); 
    }

}
