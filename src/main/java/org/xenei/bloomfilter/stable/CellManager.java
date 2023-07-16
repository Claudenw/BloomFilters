package org.xenei.bloomfilter.stable;

import java.util.function.IntBinaryOperator;

/**
 * Some Bloom filters use counters rather than bits. In this case each counter is called a {@code cell}.
 * This interface defiens the methods on cell managers.
 * <p>
 * Some definitions:</p>
 * <ul>
 * <li>{@code cell} - A collection of bits that are interpreted as a counter.</li>
 * <li>{@code block} - A collection of cells that are handled as a single entity, usually along native number sizes.
 * (e.g. 8 (byte), 16 (short), 32 (int), 64 (long))</li>
 * </ul>
 * @since 4.5
 */
public interface CellManager extends CellProducer {

    /**
     * gets the value of the cell.
     * @param entry the cell to retrieve.
     * @return the value of the cell.
     */
    int get(int entry);

    /**
     * Sets the value of the entry to a default value. 
     * @param entry the cell to set.
     * @param value the value to set the cell to.
     * @returns {@code true} if the buffer is still valid.
     */
    boolean set(int entry, int value);

    /**
     * Decrement the value of the entry by a the value.
     * @param entry the cell to decrement.
     * @param value the value to set the cell to.
     * @returns {@code true} if the buffer is still valid.
     */
    boolean decrement(int entry, int value);
    
    /**
     * Decrement the value of the entry by the value or sets to zero if the 
     * cell value would drop below zero.
     * @param entry the cell to decrement.
     * @param value the value to set the cell to.
     */
    void safeDecrement(int entry, int value);

    /**
     * Increment the value of the entry by the value.
     * @param entry the cell to decrement.
     * @param value the value to set the cell to.
     * @returns {@code true} if the buffer is still valid.
     */
    boolean increment(int entry, int value);

    /**
     * Increment the value of the entry by the value or set the value to zero if it exceeds the
     * max cell value.
     * @param entry the cell to decrement.
     * @param value the value to set the cell to.
     */
    void safeIncrement(int entry, int value);

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
     * Makes a copy of the CellManager.
     * @return A copy of the CellManager.
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
    
    /**
     * Get the shape of the cells that this manager is managing.
     * @return the cell shape.
     */
    CellShape getCellShape();
 
}