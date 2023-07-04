package org.xenei.bloomfilter.stable;

import java.util.Arrays;
import java.util.function.IntBinaryOperator;

import org.apache.commons.collections4.bloomfilter.BitMap;

public abstract class AbstractCellManager implements CellManager {
    /**
     * The BufferShape of the buffer.
     */
    protected final CellShape shape;
    /**
     * The buffer.
     */
    protected final byte[] buffer;
    /**
     * The valid flag.
     */
    protected boolean valid;

    /**
     * Get a buffer manager based on the shape.
     * @param shape the BufferShape to create.
     * @return a BufferManager instance.
     */
    public static CellManager instance(CellShape shape) {
        byte entriesPerByte = (byte) (Byte.SIZE / shape.bitsPerCell());
        return (entriesPerByte == 1) ? new Simple(shape) : new Packed(shape);
    }

    private AbstractCellManager(CellShape shape, int buffSize) {
        this.shape = shape;
        this.buffer = new byte[buffSize];
    }
    
    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public void clear() {
        Arrays.fill(buffer, (byte) 0);
        valid = true;
    }

    /**
     * Convert an unsigned byte to an int.
     * @param value The unsigned value to convert.
     * @return the value as an int.
     */
//    protected int asInt(byte value) {
//        return 0xFF & value;
//    }

    /**
     * Truncate an int as to an unsigned byte value.
     * Values greater than 0xFF are truncated as value & 0xFF
     * @param value the value to truncate.
     * @return the unsigned byte value.
     */
//    protected int asInt(int value) {
//        return 0xFF & value;
//    }

    /**
     * A simple buffer that stores one byte per byte in the buffer.
     *
     */
    public static class Simple extends AbstractCellManager {
        /**
         * Constructor
         * @param shape the shape for the buffe.r
         */
        Simple(CellShape shape) {
            super(shape, shape.numberOfCells());
            
        }

        @Override
        public Simple copy() {
            Simple result = new Simple(this.shape);
            result.valid = valid;
            System.arraycopy(this.buffer, 0, result.buffer, 0, result.buffer.length);
            return result;
        }

        @Override
        public int get(int entry) {
            return CellShape.asInt(buffer[entry]);
        }

        @Override
        public void set(int entry) {
            buffer[entry] = CellShape.asByte(shape.resetValue());
        }

        @Override
        public void decrement(int entry) {
            if (buffer[entry] != 0) {
                buffer[entry] = CellShape.asByte(CellShape.asInt(buffer[entry]) - 1);
            }
        }

        @Override
        public boolean isSet(int entry) {
            return buffer[entry] != 0;
        }

        @Override
        public void func(int entry, int value, IntBinaryOperator f) {
            buffer[entry] = CellShape.asByte(f.applyAsInt(get(entry), value));
        }
    }

    /**
     * An implementation of BufferManager that packs multiple cells into a single byte of the buffer.
     *
     */
    public static class Packed extends AbstractCellManager {
        /**
         * position of the Position value in a result array.
         */
        private static final byte POSITION = 0;
        /**
         * position of the Offset value in a result array;
         */
        private static final byte OFFSET = 1;

        /**
         * The mask for a single cell.
         */
        private final byte mask;

        /**
         * Creates an empty packed buffer.
         * @param shape the Buffer shape.
         */
        Packed(CellShape shape) {
            super(shape, (int) Math.ceil(shape.numberOfCells() * 1.0 / shape.cellsPerByte()));
            this.mask = (byte) ((1 << shape.bitsPerCell()) - 1);
        }

        @Override
        public Packed copy() {
            Packed result = new Packed(this.shape);
            result.valid = valid;
            System.arraycopy(this.buffer, 0, result.buffer, 0, result.buffer.length);
            return result;
        }

        /**
         * Returns a result array of position and offset for the position of an entry.
         * 
         * @param entry the entry to locate.
         * @return int[] of position and offset
         */
        private int[] location(int entry) {
            return new int[] { entry / shape.cellsPerByte(), BitMap.mod(entry, shape.cellsPerByte()) * shape.bitsPerCell() };
        }

        /**
         * Returns value from a result array of position and offset
         * @param location a result array of position and offset
         * @return the value of the cell.
         */
        private int get(int[] location) {
            return ((mask << location[OFFSET]) & buffer[location[POSITION]]) >> location[OFFSET];
        }

        @Override
        public int get(int entry) {
            return get(location(entry));

        }

        /**
         * Sets a value for a result array of position and offset.
         * @param location a result array of position and offset
         * @param rawValue the value to set at the location.
         */
        private void set(int[] location, int rawValue) {
            int value = rawValue << location[OFFSET];
            int reverseMask = ~(mask << location[OFFSET]);
            buffer[location[POSITION]] = CellShape.asByte((buffer[location[POSITION]] & reverseMask) | value);
        }

        @Override
        public void set(int entry) {
            set(location(entry), shape.resetValue());
        }

        @Override
        public void decrement(int entry) {
            func(entry, 1, (x, y) -> x > 0 ? x - y : 0);
        }

        @Override
        public boolean isSet(int entry) {
            int[] location = location(entry);
            return (buffer[location[POSITION]] & (mask << location[OFFSET])) != 0;
        }

        @Override
        public void func(int entry, int value, IntBinaryOperator f) {
            int[] location = location(entry);
            set(location, f.applyAsInt(get(location), value));
        }
    }
}