package org.xenei.bloomfilter.stable;

import org.apache.commons.collections4.bloomfilter.Shape;

public interface CellShape {

    
    /**
     * The number of bits per cell.
     */
    byte bitsPerCell();
    
    /**
     * The number of cells in a single byte.
     * @param blockSize the size of a block of cells in bits.
     */
    default int cellsPerBlock(int blockSize) {
        return (blockSize / bitsPerCell());
    }

    default int blocksRequired(int blockSize) {
        // cells / cells/block = blocks
        return (int) Math.ceil( numberOfCells() * 1.0 / cellsPerBlock(blockSize));
    }

    default int maxValue() {
        return (1 << bitsPerCell())-1;
    }

    default int numberOfCells() {
        return getShape().getNumberOfBits();
    }
    
    /**
     * Convert an int to an unsigned byte.
     * Values greater than 0xFF are truncated by value & 0xFF.
     * @param value the value to convert.
     * @return the byte version of the value.
     */
    public static byte asByte(int value) {
        return (byte) (0xFF & value);
    }
    
    /**
     * Convert an unsigned byte to an int value.
     * @param value the value to truncate.
     * @return the unsigned byte value.
     */
    public static int asInt(byte value) {
        return 0xFF & value;
    }
    
    public Shape getShape();
}
