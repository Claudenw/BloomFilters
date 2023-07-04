package org.xenei.bloomfilter.stable;

public interface CellShape {
    /**
     * Gets the number of entries in the Buffer.
     * This is also known as {@code m}.
     *
     * @return the number of entries in the Buffer ({@code m}).
     */
    int numberOfCells();
    
    /**
     * The number of bits per cell.
     */
    byte bitsPerCell();
    
    /**
     * The number of cells in a single byte.
     */
    default byte cellsPerByte() {
        return (byte) (Byte.SIZE / bitsPerCell());
    }
    /**
     * The unsigned byte value to set the cell when it is enabled. By default resetValue = (2^bitsPerCell)-1.
     * Must be in the range [0,255].
     */
    int resetValue();
    
    /**
     * Test that the settings of the shape are reasonable.
     * @param shape
     */
    public static void verifySettings(CellShape shape) {
        if (shape.resetValue() > 255 || shape.resetValue() < 1) {
            throw new IllegalStateException("reset value must be in the range [1,255]");
        }
        if (Math.pow(2, shape.bitsPerCell()) < shape.resetValue()) {
            throw new IllegalStateException( String.format( "2^%s > %s", shape.bitsPerCell(), shape.resetValue()));
        }
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
}
