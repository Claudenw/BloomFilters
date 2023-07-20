package org.xenei.bloomfilter.stable;

import java.util.Objects;
import java.util.function.LongPredicate;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.CellProducer;
import org.apache.commons.collections4.bloomfilter.CountingBloomFilter;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

public class VariableCountingBloomFilter implements CountingBloomFilter {

    private CellManager cellManager;

    public VariableCountingBloomFilter(CellManager manager) {
        cellManager = manager;
    }

    @Override
    public int characteristics() {
        return 0;
    }

    @Override
    public int getMaxCell() {
        return cellManager.getCellShape().maxValue();
    }

    @Override
    public Shape getShape() {
        return cellManager.getCellShape().getShape();
    }

    @Override
    public void clear() {
        cellManager.clear();
    }

    @Override
    public boolean contains(IndexProducer indexProducer) {
        return indexProducer.forEachIndex(cellManager::isSet);
    }

    @Override
    public int cardinality() {
        return cellManager.cardinality();
    }

    @Override
    public boolean forEachBitMap(LongPredicate consumer) {
        Objects.requireNonNull(consumer, "consumer");
        int numberOfCells = cellManager.getCellShape().numberOfCells();
        final int blocksm1 = BitMap.numberOfBitMaps(numberOfCells) - 1;
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
        for (int k = 0; i < numberOfCells; k++) {
            if (cellManager.isSet(i++)) {
                value |= BitMap.getLongBit(k);
            }
        }
        return consumer.test(value);
    }

    @Override
    public boolean forEachCell(CellConsumer consumer) {
        return cellManager.forEachCell(consumer);
    }

    @Override
    public boolean isValid() {
        return cellManager.isValid();
    }

    @Override
    public boolean add(CellProducer other) {
        return other.forEachCell(cellManager::increment);
    }

    @Override
    public boolean subtract(CellProducer other) {
        return other.forEachCell(cellManager::decrement);
    }

    @Override
    public CountingBloomFilter copy() {
        return new VariableCountingBloomFilter(this.cellManager.copy());
    }

    @Override
    public boolean merge(final IndexProducer indexProducer) {
        Objects.requireNonNull(indexProducer, "indexProducer");
        try {
            return add(CellProducer.from(indexProducer));
        } catch (final IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                    String.format("Filter only accepts values in the [0,%d) range", getShape().getNumberOfBits()), e);
        }
    }

}
