package org.xenei.bloomfilter.stable;

import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;

/**
 * Based http://webdocs.cs.ualberta.ca/~drafiei/papers/DupDet06Sigmod.pdf
 *
 */
public class StableBloomFilter implements BloomFilter {
    private final StableShape shape;
    private final FastPseudoRandomInt idxFactory;
    private final CellManager cellManager;
    private int cardinality;

    /**
     * Create a stable Bloom filter.
     * @param shape the Stable shape.
     */
    public StableBloomFilter(StableShape shape) {
        this(shape, new LongArrayCellManager(shape));
    }

    private StableBloomFilter(StableShape shape, CellManager buffer) {
        this.shape = shape;
        this.idxFactory = new FastPseudoRandomInt();
        this.cellManager = buffer;
        this.cardinality = -1;
    }

    /**
     * Gets the stableShape for this Bloom filter.
     * @return the Stable shape.
     */
    public StableShape getStableShape() {
        return shape;
    }

    @Override
    public int characteristics() {
        return SPARSE;
    }

    @Override
    public Shape getShape() {
        return shape.getShape();
    }

    @Override
    public void clear() {
        cellManager.clear();
        cardinality = 0;
    }

    @Override
    public boolean contains(IndexProducer indexProducer) {
        return indexProducer.forEachIndex(x -> {
            return cellManager.isSet(x);
        });
    }

    @Override
    public int cardinality() {
        int result = cardinality;
        if (result < 0) {
            int[] accumulator = {0};
            cellManager.forEachCell( c -> { if (c!=0) { accumulator[0]++;} return true;});
            cardinality = result = accumulator[0];
        }
        return result;
    }

    @Override
    public boolean merge(final IndexProducer indexProducer) {
        Objects.requireNonNull(indexProducer, "indexProducer");
        decrement();
        return indexProducer.forEachIndex(x -> {
            if (x >= shape.numberOfCells() || x < 0) {
                throw new IllegalArgumentException(
                        String.format("Filter only accepts values in the [0,%d) range", getShape().getNumberOfBits()));
            }
            cellManager.set(x,shape.resetValue());
            return true;
        });
    }

    @Override
    public boolean merge(final BitMapProducer bitMapProducer) {
        Objects.requireNonNull(bitMapProducer, "bitMapProducer");
        return this.merge(IndexProducer.fromBitMapProducer(bitMapProducer));
    }

    @Override
    public boolean merge(final BloomFilter other) {
        return merge((IndexProducer) other);
    }

    @Override
    public boolean merge(final Hasher hasher) {
        Objects.requireNonNull(hasher, "hasher");
        return merge(hasher.uniqueIndices(getShape()));
    }

    @Override
    public boolean forEachBitMap(LongPredicate consumer) {
        Objects.requireNonNull(consumer, "consumer");
        final int blocksm1 = BitMap.numberOfBitMaps(shape.numberOfCells()) - 1;
        int i = 0;
        long value;
        // must break final block separate as the number of bits may not fall on the
        // long boundary
        for (int j = 0; j < blocksm1; j++) {
            value = 0;
            for (int k = 0; k < Long.SIZE; k++) {
                if (cellManager.isSet(i++)) {
                    value |= BitMap.getLongBit(k);
                }
            }
            if (!consumer.test(value)) {
                return false;
            }
        }
        // Final block
        value = 0;
        for (int k = 0; i < shape.numberOfCells(); k++) {
            if (cellManager.isSet(i++)) {
                value |= BitMap.getLongBit(k);
            }
        }
        return consumer.test(value);
    }

    @Override
    public boolean forEachIndex(final IntPredicate consumer) {
        Objects.requireNonNull(consumer, "consumer");
        for (int i = 0; i < shape.numberOfCells(); i++) {
            if (cellManager.isSet(i) && !consumer.test(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public StableBloomFilter copy() {
        return new StableBloomFilter( this.shape, this.cellManager.copy() );
    }
    
    /**
     * Flatten the stable filter to a SimpleBloomFilter.
     * @return a SimpleBloomFilter with equivalent bits enabled.
     */
    public BloomFilter flatten() {
        BloomFilter bf = new SimpleBloomFilter(this.shape.getShape());
        bf.merge(this);
        return bf;
    }

    private void decrement() {
        cardinality = -1;
        idxFactory.indices(shape.decrementShape).forEachIndex(x -> {cellManager.safeDecrement(x, 1);return true;} );
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
    public int estimateIntersection(final BloomFilter other) {
        Objects.requireNonNull(other, "other");
        double eThis = getShape().estimateN(cardinality());
        double eOther = getShape().estimateN(other.cardinality());
        if (Double.isInfinite(eThis) && Double.isInfinite(eOther)) {
            // if both are infinite the union is infinite and we return Integer.MAX_VALUE
            return Integer.MAX_VALUE;
        }
        long estimate;
        // if one is infinite the intersection is the other.
        if (Double.isInfinite(eThis)) {
            estimate = Math.round(eOther);
        } else if (Double.isInfinite(eOther)) {
            estimate = Math.round(eThis);
        } else {
            // call flatten() not copy()
            BloomFilter union = this.flatten();
            union.merge(other);
            double eUnion = getShape().estimateN(union.cardinality());
            if (Double.isInfinite(eUnion)) {
                throw new IllegalArgumentException("The estimated N for the union of the filters is infinite");
            }
            // maximum estimate value using integer values is: 46144189292 thus
            // eThis + eOther can not overflow the long value.
            estimate = Math.round(eThis + eOther - eUnion);
            estimate = estimate < 0 ? 0 : estimate;
        }
        return estimate>Integer.MAX_VALUE?Integer.MAX_VALUE:(int) estimate;
    }
}
