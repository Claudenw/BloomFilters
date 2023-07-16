package org.xenei.bloomfilter.stable;

import java.util.Arrays;
import java.util.function.IntPredicate;

import org.apache.commons.collections4.bloomfilter.BitMap;

public class LongArrayCellManager implements CellManager {

    /**
     * The BufferShape of the buffer.
     */
    protected final CellShape shape;

    /**
     * Number of cells in a long Block.
     */
    protected final int cellsPerBlock;
    /**
     * The buffer.
     */
    protected final long[] buffer;
    /**
     * The valid flag.
     */
    protected boolean invalid;
    
    protected int cardinality;

    public LongArrayCellManager(CellShape shape) {
        this(shape, new long[shape.blocksRequired(Long.SIZE)], false, 0);
    }

    private LongArrayCellManager(CellShape shape, long[] buffer, boolean invalid, int cardinality) {
        this.shape = shape;
        this.invalid = invalid;
        this.cardinality = cardinality;
        this.cellsPerBlock = shape.cellsPerBlock(Long.SIZE);
        this.buffer = buffer;
    }

    protected int getBlock(int idx) {
        return idx / cellsPerBlock;
    }

    protected long getMask(int idx) {
        return this.shape.cellMask() << (BitMap.mod(idx, cellsPerBlock)*shape.getBitsPerCell());
    }
    
    protected long getMask(int idx, int value) {
        return value==0?0:(this.shape.cellMask() & value) << (BitMap.mod(idx, cellsPerBlock)*shape.getBitsPerCell());
    }

    @Override
    public boolean set(int idx, int value) {
        return setLong(idx, value);
    }

    private long getLong(int idx) {
        long v = buffer[getBlock(idx)] & getMask(idx);
        int shift = BitMap.mod(idx, cellsPerBlock) * shape.getBitsPerCell();
        return v >> shift & 0xffff_ffff;
    }

    @Override
    public int get(int idx) {
        return (int) getLong(idx);
    }

    @Override
    public boolean isSet(int idx) {
        return (buffer[getBlock(idx)] & getMask(idx)) != 0;
    }

    private boolean setLong(int idx, long value) {
        invalid |= !valueInRange(value);
        long offmask = getMask(idx);
        int block = getBlock(idx);
        long current = buffer[block] & offmask;
        if (current != 0 && value == 0) {
            cardinality--;
        }
        if (current == 0 && value != 0) {
            cardinality++;
        }
        buffer[block] = (buffer[block] & ~offmask) | getMask(idx, (int) value);
        return isValid();
    }

    @Override
    public boolean increment(int idx, int value) {
        setLong(idx, getLong(idx) + value);
        return isValid();
    }

    private boolean valueInRange(long value) {
        return value >= 0 && value <= shape.maxValue();
    }

    @Override
    public void safeIncrement(int idx, int value) {
        long val = getLong(idx);
        if (val != shape.cellMask()) {
            val += value;
            setLong(idx, valueInRange(val) ? 0 : val);
        }
    }

    @Override
    public boolean decrement(int idx, int value) {
        return setLong(idx, getLong(idx) - value);
    }

    @Override
    public void safeDecrement(int idx, int value) {
        long val = getLong(idx);
        if (val != 0) {
            val -= value;
            setLong(idx, valueInRange(val) ? 0 : val);
        }
    }

    @Override
    public boolean isValid() {
        return !invalid;
    }

    @Override
    public void clear() {
        Arrays.fill(buffer, 0);
        invalid = false;
        cardinality = 0;
    }

    @Override
    public CellShape getCellShape() {
        return shape;
    }

    @Override
    public LongArrayCellManager copy() {
        return new LongArrayCellManager(this.shape, Arrays.copyOf(buffer, buffer.length), invalid, cardinality);
    }

    @Override
    public boolean forEachCell(IntPredicate predicate) {
        for (int idx = 0; idx < shape.getShape().getNumberOfBits(); idx++) {
            if (!predicate.test(get(idx))) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public int cardinality() {
        return cardinality;
    }
}