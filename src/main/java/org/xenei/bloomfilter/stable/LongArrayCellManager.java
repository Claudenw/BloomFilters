package org.xenei.bloomfilter.stable;

import java.util.Arrays;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;

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
     * The bitmap representation of the buffer.
     */
    protected final long[] bitMap;
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
        this.bitMap = new long[BitMap.numberOfBitMaps(shape.numberOfCells())];
    }

    protected int getBlock(int idx) {
        if (idx < 0 || idx > shape.getShape().getNumberOfBits()) {
            throw new IndexOutOfBoundsException( String.format( "%s not in the range [0,%s]", idx, shape.getShape().getNumberOfBits()));
        }
        return idx / cellsPerBlock;
    }

    protected long getMask(int idx) {
        return this.shape.cellMask() << (BitMap.mod(idx, cellsPerBlock) * shape.getBitsPerCell());
    }

    protected long getMask(int idx, int value) {
        return value == 0 ? 0
                : (this.shape.cellMask() & value) << (BitMap.mod(idx, cellsPerBlock) * shape.getBitsPerCell());
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
        if (!invalid) {
            long offmask = getMask(idx);
            int block = getBlock(idx);
            long current = buffer[block] & offmask;
            if (current != 0 && value == 0) {
                cardinality--;
                bitMap[BitMap.getLongIndex(idx)] &= ~BitMap.getLongBit(idx);
            }
            if (current == 0 && value != 0) {
                cardinality++;
                bitMap[BitMap.getLongIndex(idx)] |= BitMap.getLongBit(idx);
            }
            buffer[block] = (buffer[block] & ~offmask) | getMask(idx, (int) value);
        }
        return isValid();
    }

    @Override
    public boolean increment(int idx, int value) {
        return setLong(idx, getLong(idx) + value);
    }

    private boolean valueInRange(long value) {
        return value >= 0 && value <= shape.maxValue();
    }

    @Override
    public void safeIncrement(int idx, int value) {
        long val = getLong(idx);
        if (val < shape.maxValue()) {
            val += value;
            setLong(idx, valueInRange(val) ? val : shape.maxValue());
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
            setLong(idx, valueInRange(val) ? val : 0 );
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
    public boolean forEachCell(CellConsumer consumer) {
        for (int idx = 0; idx < shape.getShape().getNumberOfBits(); idx++) {
            int val = get(idx);
            if (val != 0 && !consumer.test(idx, get(idx))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int cardinality() {
        return cardinality;
    }
    
    public boolean isEmpty() {
        return cardinality == 0;
    }
    
    @Override
    public BitMapProducer getBitmap() {
        return BitMapProducer.fromBitMapArray(bitMap);
    }
}