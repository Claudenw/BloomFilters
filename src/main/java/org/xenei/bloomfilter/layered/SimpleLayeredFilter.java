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
    private final Predicate<BloomFilter> extendCheck;
    
    public SimpleLayeredFilter(final Shape shape, int maxDepth) {
        this( shape, maxDepth, (x)-> false );
    }
    
    public SimpleLayeredFilter(final Shape shape, int maxDepth, Predicate<BloomFilter> extendCheck) {
        super(shape, maxDepth);
        this.filters = new ArrayList<BloomFilter>();
        this.extendCheck = extendCheck;
        next();
    }

    @Override
    public BloomFilter copy() {
        BloomFilter merged = new SimpleBloomFilter(shape);
        if (forEachBloomFilter( merged::merge )) {
            return merged;
        }
        return new SparseBloomFilter(shape);
    }


    @Override
    public void clear() {
        this.filters.clear();
        next();
    }

    protected boolean doContains(BloomFilter bf) {
        return filters.stream().anyMatch( x -> x.contains(bf));
    }
    
    protected boolean doMerge(BloomFilter bf) {
        BloomFilter target = filters.get(filters.size()-1);
        if (extendCheck.test(target)) {
            next();
            target = filters.get(filters.size()-1);
        }
        return target.merge(bf);
      
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
            if (bloomFilterPredicate.test(bf)) {
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
