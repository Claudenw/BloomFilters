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
    private final BufferManager buffer;
    private int cardinality;

    public StableBloomFilter(StableShape shape) {
        this(shape, AbstractBufferManager.instance(shape));
    }

    private StableBloomFilter(StableShape shape, BufferManager buffer) {
        this.shape = shape;
        this.idxFactory = new FastPseudoRandomInt();
        this.buffer = buffer;
        this.cardinality = -1;
    }

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
        buffer.clear();
        cardinality = 0;
    }

    @Override
    public boolean contains(IndexProducer indexProducer) {
        return indexProducer.forEachIndex(x -> {
            return buffer.isSet(x);
        });
    }

    @Override
    public int cardinality() {
        if (cardinality < 0) {
            int result = 0;
            for (int i = 0; i < shape.getNumberOfEntries(); i++) {
                if (buffer.isSet(i)) {
                    result++;
                }
            }
            cardinality = result;
        }
        return cardinality;
    }

    @Override
    public boolean merge(final IndexProducer indexProducer) {
        Objects.requireNonNull(indexProducer, "indexProducer");
        decrement();
        return indexProducer.forEachIndex(x -> {
            if (x >= shape.getNumberOfEntries() || x < 0) {
                throw new IllegalArgumentException(
                        String.format("Filter only accepts values in the [0,%d) range", getShape().getNumberOfBits()));
            }
            buffer.set(x);
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
        final int blocksm1 = BitMap.numberOfBitMaps(shape.getNumberOfEntries()) - 1;
        int i = 0;
        long value;
        // must break final block separate as the number of bits may not fall on the
        // long boundary
        for (int j = 0; j < blocksm1; j++) {
            value = 0;
            for (int k = 0; k < Long.SIZE; k++) {
                if (buffer.isSet(i++)) {
                    value |= BitMap.getLongBit(k);
                }
            }
            if (!consumer.test(value)) {
                return false;
            }
        }
        // Final block
        value = 0;
        for (int k = 0; i < shape.getNumberOfEntries(); k++) {
            if (buffer.isSet(i++)) {
                value |= BitMap.getLongBit(k);
            }
        }
        return consumer.test(value);
    }

    @Override
    public boolean forEachIndex(final IntPredicate consumer) {
        Objects.requireNonNull(consumer, "consumer");
        for (int i = 0; i < shape.getNumberOfEntries(); i++) {
            if (buffer.isSet(i) && !consumer.test(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public StableBloomFilter copy() {
        return new StableBloomFilter( this.shape, this.buffer.copy() );
    }
    
    public BloomFilter flatten() {
        BloomFilter bf = new SimpleBloomFilter(this.shape.getShape());
        bf.merge(this);
        return bf;
    }

    private void decrement() {
        cardinality = -1;
        idxFactory.indices(shape.decrementShape).forEachIndex(x -> {
            buffer.decrement(x);
            return true;
        });
    }
}
