package org.xenei.bloomfilter.layered;

import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;

public abstract class LayeredBloomFilter implements BloomFilter {
    protected final Shape shape;
    protected final int maxDepth;
    
    LayeredBloomFilter(Shape shape, int maxDepth) {
        this.shape = shape;
        this.maxDepth = maxDepth;
    }
    
    abstract void next();
    abstract int getDepth();
    abstract void clear(int level);
    abstract boolean forEachBloomFilter(Predicate<BloomFilter> bloomFilterPredicate);

    abstract protected boolean doContains(BloomFilter bf);
    
    abstract protected boolean doMerge(BloomFilter bf);

    /**
     * Returns {@code true} if this filter contains the specified filter.
     *
     * <p>Specifically this
     * returns {@code true} if this filter is enabled for all bits that are enabled in the
     * {@code other} filter. Using the bit representations this is
     * effectively {@code (this AND other) == other}.</p>
     *
     * @param other the other Bloom filter
     * @return true if all enabled bits in the other filter are enabled in this filter.
     */
    public boolean contains(final BloomFilter other) {
        return doContains( other );
    }

    protected BloomFilter createFilter(final Hasher hasher) {
        SimpleBloomFilter bf = new SimpleBloomFilter(shape);
        bf.merge(hasher);
        return bf;
    }
    
    protected BloomFilter createFilter(final IndexProducer indexProducer) {
        SimpleBloomFilter bf = new SimpleBloomFilter(shape);
        bf.merge(indexProducer);
        return bf;
    }
    
    protected BloomFilter createFilter(final BitMapProducer bitMapProducer) {
        SimpleBloomFilter bf = new SimpleBloomFilter(shape);
        bf.merge(bitMapProducer);
        return bf;
    }
    
    @Override
    public int characteristics() {
        return 0;
    }
    
    public final Shape getShape() {
        return shape;
    }

    /**
     * Returns {@code true} if this filter contains the bits specified in the hasher.
     *
     * <p>Specifically this returns {@code true} if this filter is enabled for all bit indexes
     * identified by the {@code hasher}. Using the bit map representations this is
     * effectively {@code (this AND hasher) == hasher}.</p>
     *
     * @param hasher the hasher to provide the indexes
     * @return true if this filter is enabled for all bits specified by the hasher
     */
    public boolean contains(final Hasher hasher) {
        return doContains(createFilter(hasher));
    }

    /**
     * Returns {@code true} if this filter contains the indices specified IndexProducer.
     *
     * <p>Specifically this returns {@code true} if this filter is enabled for all bit indexes
     * identified by the {@code IndexProducer}.</p>
     *
     * @param indexProducer the IndexProducer to provide the indexes
     * @return {@code true} if this filter is enabled for all bits specified by the IndexProducer
     */
    public boolean contains(IndexProducer indexProducer) {
        return doContains(createFilter(indexProducer));
    }
    
    
    @Override
    public boolean merge(IndexProducer indexProducer) {
        return doMerge(createFilter(indexProducer));
        
    }

    @Override
    public boolean merge(BitMapProducer bitMapProducer) {
        return doMerge(createFilter(bitMapProducer));
    }
    
    @Override
    public boolean merge(BloomFilter filter) {
        return doMerge(filter);
    }
    
    @Override
    public boolean forEachIndex(IntPredicate predicate) {
        return forEachBloomFilter( bf -> bf.forEachIndex(predicate));
    }

    @Override
    public boolean forEachBitMap(LongPredicate predicate) {
        BloomFilter merged = new SimpleBloomFilter(shape);
        if (forEachBloomFilter( merged::merge )) {
            return merged.forEachBitMap(predicate);
        }
        return false;
    }
}
