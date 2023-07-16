package org.xenei.bloomfilter.stable;

import org.apache.commons.collections4.bloomfilter.Shape;

public class CellShape {

    private final int bitsPerCell;
    private final int maxValue;
    private final Shape shape;

    protected static void validateMaxValue(int maxValue) {
        if (maxValue < 1) {
            throw new IllegalArgumentException("maxValue must be greater than 0");
        }
    }

    protected static int calcCellShape(int maxValue) {
        long limit = 1;
        for (int i = 1; i < Integer.SIZE; i++) {
            limit = limit << 1;
            if (limit > maxValue) {
                return i;
            }
        }
        throw new IllegalStateException("should not get here");
    }

    public static CellShape fromMaxValue(Shape shape, int maxValue) {
        validateMaxValue(maxValue);
        return new CellShape(shape, maxValue, calcCellShape(maxValue));
    }

    private static void validateBitsPerCell(int bitsPerCell) {
        if (bitsPerCell >= Integer.SIZE) {
            throw new IllegalArgumentException("bitsPerCell must be less than " + Integer.SIZE);
        }
        if (bitsPerCell < 1) {
            throw new IllegalArgumentException("bitsPerCell must be greater than 0");
        }
    }

    public static CellShape fromBitsPerCell(Shape shape, int bitsPerCell) {
        validateBitsPerCell(bitsPerCell);
        return new CellShape(shape, (1 << bitsPerCell) - 1, bitsPerCell);
    }

    protected CellShape(Shape shape, int maxValue, int bitsPerCell) {
        validateMaxValue(maxValue);
        validateBitsPerCell(bitsPerCell);
        if (maxValue >= 1L << bitsPerCell) {
            throw new IllegalStateException(
                    String.format("reset value must be in the range [1,%s)", 1L << bitsPerCell));
        }
        this.shape = shape;
        this.maxValue = maxValue;
        this.bitsPerCell = bitsPerCell;
    }

    /**
     * The number of cells in a single byte.
     * 
     * @param blockSize the size of a block of cells in bits.
     */
    public int cellsPerBlock(int blockSize) {
        if (blockSize < bitsPerCell) {
            throw new IllegalArgumentException(
                    String.format("blockSize (%s) is too small to hold cells of %s bits", blockSize, bitsPerCell));
        }
        return (blockSize / bitsPerCell);
    }

    final public int blocksRequired(int blockSize) {
        // cells / cells/block = blocks
        return (int) Math.ceil(numberOfCells() * 1.0 / cellsPerBlock(blockSize));
    }

    /**
     * Create the bit mask for a cell. This is the mask to find all the bits in a
     * cell. This value is not shifter to a cell position so it effectively is the
     * mask for cell zero.
     * 
     * @return the bit mask for a cell.
     */
    final public long cellMask() {
        return (1L << bitsPerCell) - 1;
    }

    final public int numberOfCells() {
        return getShape().getNumberOfBits();
    }

    final public int getBitsPerCell() {
        return bitsPerCell;
    }

    /**
     * The maximum value for a cell.
     * 
     * @return The maximum value allowed for a cell.
     */
    final public int maxValue() {
        return maxValue;
    };

    final public Shape getShape() {
        return shape;
    }
}
