package org.xenei.bloomfilter.stable;

import java.util.function.IntBinaryOperator;

/**
 * A manager for Bloom filter buffers where the number of bits used for a cell is in the range of [1,8] bits.
 *
 */
public interface CellManager {

    /**
     * gets the value of the cell.
     * @param entry the cell to retrieve.
     * @return the value of the cell.
     */
    int get(int entry);

    /**
     * Sets the value of the entry to a default value. 
     * @param entry the cell to set.
     */
    void set(int entry);

    /**
     * Decrement the value of the entry by a default decrement value.
     * @param entry the cell to decrement.
     */
    void decrement(int entry);

    /**
     * Tests if the cell is set.  Returns true if the cell is set.
     * @param entry the entry to check.
     * @return {@code true} if the entry is set, {@code false} otherwise.
     */
    boolean isSet(int entry);

    /**
     * Clears the entire buffer, effectively empties the Bloom filter.
     */
    void clear();

    /**
     * Applies the function to the value of the cell in the buffer and sets the value.
     * @param entry The cell to update.
     * @param value the second value to pass to the function.
     * @param f A function to operate on the current cell value and the value parameter.
     */
    void func(int entry, int value, IntBinaryOperator f);

    /**
     * Makes a copy of the buffer.
     * @return A copy of the buffer.
     */
    CellManager copy();
    
    /**
     * Are all the cells valid.
     * If a cell count goes negative or exceeds the number of bits allocated for the value the cell is said to be invalid.
     * A manager with an invalid cell is invalid. Once a manager becomes invalid it will always be invalid until
     * {@code clear()} is called.
     * @see #clear()
     * @return true if this manager is valid.
     */
    boolean isValid();
    
}