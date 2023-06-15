package org.xenei.bloomfilter.layered;

import java.util.Objects;
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
    public static final Predicate<LayeredBloomFilter> ADVANCE_ON_POPULATED = x -> {return !x.target().forEachBitMap( y -> y==0);};
    public static final Predicate<LayeredBloomFilter> NEVER_ADVANCE = x-> false;
    
    protected final Shape shape;
    protected final int maxDepth;
    protected final Predicate<LayeredBloomFilter> extendCheck;
    
    LayeredBloomFilter(Shape shape, int maxDepth, Predicate<LayeredBloomFilter> extendCheck) {
        this.shape = shape;
        this.maxDepth = maxDepth;
        this.extendCheck = extendCheck;
    }
    
    abstract public void next();
    abstract public int getDepth();
    abstract public void clear(int level);
    abstract public BloomFilter target();
    abstract public boolean forEachBloomFilter(Predicate<BloomFilter> bloomFilterPredicate);

    abstract protected boolean doContains(BloomFilter bf);
 
    
    public BloomFilter flatten() {
        BloomFilter bf = new SimpleBloomFilter(shape);
        forEachBloomFilter( bf::merge );
        return bf;
    }

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
    @Override
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
    public boolean merge(BloomFilter bf) {
        if (extendCheck.test(this)) {
            next();
        }
        return target().merge(bf);
    }

    @Override
    public boolean merge(IndexProducer indexProducer) {
        if (extendCheck.test(this)) {
            next();
        }
        return target().merge(indexProducer);
    }
    
    @Override
    public boolean merge(BitMapProducer bitMapProducer) {
        if (extendCheck.test(this)) {
            next();
        }
        return target().merge(bitMapProducer);
    }

    
    @Override
    public boolean forEachIndex(IntPredicate predicate) {
        return forEachBloomFilter( bf -> bf.forEachIndex(predicate));
    }

    @Override
    public boolean forEachBitMap(LongPredicate predicate) {
        BloomFilter merged = new SimpleBloomFilter(shape);
        if (forEachBloomFilter( merged::merge )) {
            if (!merged.forEachBitMap(predicate)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Estimates the number of items in the Bloom filter.
     *
     * <p>By default this is the rounding of the {@code Shape.estimateN(cardinality)} calculation for the
     * shape and cardinality of this filter.</p>
     *
     * <p>This produces an estimate roughly equivalent to the number of Hashers that have been merged into the filter
     * by rounding the value from the calculation described in the {@link Shape} class javadoc.</p>
     *
     * <p><em>Note:</em></p>
     * <ul>
     * <li>if cardinality == numberOfBits, then result is Integer.MAX_VALUE.</li>
     * <li>if cardinality &gt; numberOfBits, then an IllegalArgumentException is thrown.</li>
     * </ul>
     *
     * @return an estimate of the number of items in the bloom filter.  Will return Integer.MAX_VALUE if the
     * estimate is larger than Integer.MAX_VALUE.
     * @throws IllegalArgumentException if the cardinality is &gt; numberOfBits as defined in Shape.
     * @see Shape#estimateN(int)
     * @see Shape
     */
    public int estimateN() {
        long[] accum = { 0 };
        forEachBloomFilter( bf -> {accum[0] += bf.estimateN();return accum[0] <= Integer.MAX_VALUE;});
        return accum[0] < Integer.MAX_VALUE ? (int) accum[0] : Integer.MAX_VALUE;
    }

    /**
     * Estimates the number of items in the union of this Bloom filter with the other bloom filter.
     *
     * <p>This produces an estimate roughly equivalent to the number of unique Hashers that have been merged into either
     * of the filters by rounding the value from the calculation described in the {@link Shape} class javadoc.</p>
     *
     * <p><em>{@code estimateUnion} should only be called with Bloom filters of the same Shape.  If called on Bloom
     * filters of differing shape this method is not symmetric. If {@code other} has more bits an {@code IllegalArgumentException}
     * may be thrown.</em></p>
     *
     * @param other The other Bloom filter
     * @return an estimate of the number of items in the union.  Will return Integer.MAX_VALUE if the
     * estimate is larger than Integer.MAX_VALUE.
     * @see #estimateN()
     * @see Shape
     */
    @Override
    public int estimateUnion(final BloomFilter other) {
        Objects.requireNonNull(other, "other");
        long[] result = { 0 };
        if (other instanceof LayeredBloomFilter) {           
            ((LayeredBloomFilter) other).forEachBloomFilter( bf -> {result[0] += this.estimateUnion(bf); return result[0]<Integer.MAX_VALUE;} );
        } else {
            forEachBloomFilter( bf -> {result[0] += bf.estimateUnion(other); return result[0]<Integer.MAX_VALUE;} );
        }
        return result[0]<Integer.MAX_VALUE ? (int)result[0] : Integer.MAX_VALUE;
    }

    /**
     * Estimates the number of items in the intersection of this Bloom filter with the other bloom filter.
     *
     * <p>This method produces estimate is roughly equivalent to the number of unique Hashers that have been merged into both
     * of the filters by rounding the value from the calculation described in the {@link Shape} class javadoc.</p>
     *
     * <p><em>{@code estimateIntersection} should only be called with Bloom filters of the same Shape.  If called on Bloom
     * filters of differing shape this method is not symmetric. If {@code other} has more bits an {@code IllegalArgumentException}
     * may be thrown.</em></p>
     *
     * @param other The other Bloom filter
     * @return an estimate of the number of items in the intersection. If the calculated estimate is larger than Integer.MAX_VALUE then MAX_VALUE is returned.
     * @throws IllegalArgumentException if the estimated N for the union of the filters is infinite.
     * @see #estimateN()
     * @see Shape
     */
    @Override
    public int estimateIntersection(final BloomFilter other) {
        Objects.requireNonNull(other, "other");
        long eThis = estimateN();
        long eOther = other.estimateN();
        if (eThis == Integer.MAX_VALUE && eOther == Integer.MAX_VALUE) {
            // if both are infinite the union is infinite and we return Integer.MAX_VALUE
            return Integer.MAX_VALUE;
        }
        long estimate;
        // if one is infinite the intersection is the other.
        if (eThis == Integer.MAX_VALUE) {
            estimate = eOther;;
        } else if (eOther == Integer.MAX_VALUE) {
            estimate = eThis;
        } else {
            long eUnion = estimateUnion( other );
            if (eUnion == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("The estimated N for the union of the filters is infinite");
            }
            // maximum estimate value using integer values is: 46144189292 thus
            // eThis + eOther can not overflow the long value.
            estimate = eThis + eOther - eUnion;
            estimate = estimate < 0 ? 0 : estimate;
        }
        return estimate>Integer.MAX_VALUE?Integer.MAX_VALUE:(int) estimate;
    }
}
