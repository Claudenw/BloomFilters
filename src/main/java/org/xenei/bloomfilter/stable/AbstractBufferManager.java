package org.xenei.bloomfilter.stable;

import java.util.Arrays;
import java.util.function.IntBinaryOperator;

import org.apache.commons.collections4.bloomfilter.BitMap;

public abstract class AbstractBufferManager implements BufferManager {

    protected final StableShape shape;
    protected final byte[] buffer;

    public static BufferManager instance(StableShape shape) {
        byte entriesPerByte = (byte) (Byte.SIZE / shape.bitsPerCell);
        return (entriesPerByte == 1) ? new Simple(shape) : new Packed(shape);
    }

    private AbstractBufferManager(StableShape shape, int buffSize) {
        this.shape = shape;
        this.buffer = new byte[buffSize];
    }

    @Override
    public void clear() {
        Arrays.fill(buffer, (byte) 0);
    }

    protected byte asByte(int value) {
        return (byte) (0xFF & value);
    }

    protected int asInt(byte value) {
        return 0xFF & value;
    }

    protected int asInt(int value) {
        return 0xFF & value;
    }

    public static class Simple extends AbstractBufferManager {

        Simple(StableShape shape) {
            super(shape, shape.getNumberOfEntries());
        }

        @Override
        public Simple copy() {
            Simple result = new Simple(this.shape);
            System.arraycopy(this.buffer, 0, result.buffer, 0, result.buffer.length);
            return result;
        }

        @Override
        public int get(int entry) {
            return asInt(buffer[entry]);
        }

        @Override
        public void set(int entry) {
            buffer[entry] = asByte(shape.resetValue);
        }

        @Override
        public void decrement(int entry) {
            if (buffer[entry] != 0) {
                buffer[entry] = asByte(asInt(buffer[entry]) - 1);
            }
        }

        @Override
        public boolean isSet(int entry) {
            return buffer[entry] != 0;
        }

        @Override
        public void func(int entry, int value, IntBinaryOperator f) {
            buffer[entry] = asByte(f.applyAsInt(get(entry), value));
        }
    }

    public static class Packed extends AbstractBufferManager {
        private static final byte POSITION = 0;
        private static final byte OFFSET = 1;

        private final byte mask;

        Packed(StableShape shape) {
            super(shape, (int) Math.ceil(shape.getNumberOfEntries() * 1.0 / shape.cellsPerByte));
            this.mask = (byte) ((1 << shape.bitsPerCell) - 1);
        }

        @Override
        public Packed copy() {
            Packed result = new Packed(this.shape);
            System.arraycopy(this.buffer, 0, result.buffer, 0, result.buffer.length);
            return result;
        }

        /**
         * Returns array of position and offset.
         * 
         * @param entry the entry to locate.
         * @return int[] of position and offset
         */
        private int[] location(int entry) {
            return new int[] { entry / shape.cellsPerByte, BitMap.mod(entry, shape.cellsPerByte) * shape.bitsPerCell };
        }

        private int get(int[] location) {
            return asInt(((mask << location[OFFSET]) & buffer[location[POSITION]]) >> location[OFFSET]);
        }

        @Override
        public int get(int entry) {
            return get(location(entry));

        }

        private void set(int[] location, int rawValue) {
            int value = rawValue << location[OFFSET];
            int reverseMask = ~(mask << location[OFFSET]);
            buffer[location[POSITION]] = asByte((buffer[location[POSITION]] & reverseMask) | value);
        }

        @Override
        public void set(int entry) {
            set(location(entry), shape.resetValue);
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