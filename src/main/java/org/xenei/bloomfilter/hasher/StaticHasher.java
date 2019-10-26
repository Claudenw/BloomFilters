package org.xenei.bloomfilter.hasher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.HasherFactory.Hasher;

/**
 * A Hasher implementation that contains the index for all enabled bits for a specific shape.
 *
 */
public final class StaticHasher implements Hasher {

    /**
     * The name of this hasher.
     */
    private final String name;
    /**
     * The shape of this hasher
     */
    private final Shape shape;
    /**
     * The list of values that this hasher will return.
     */
    private final List<Integer> values;

    /**
     * Constructs the StaticHasher from a DynamicHasher and a Shape.
     * @param other the DynamicHasher to read.
     * @param shape the Shape for the resulting values.
     */
    public StaticHasher(DynamicHasher other, Shape shape) {
        this( other.getBits(shape), shape);
    }

    /**
     * Constructs a StaticHasher from an Iterator of Integers and a Shape.
     * @param iter the Iterator of Integers.
     * @param shape the Shape that the integers were generated for.
     * @throws IllegalArgumentException if any Integer is outside the range [0,shape.getNumberOfBits())
     */
    public StaticHasher(Iterator<Integer> iter, Shape shape) {
        this.name = shape.getHasherName();
        this.shape = shape;
        Set<Integer> set = new TreeSet<Integer>();
        iter.forEachRemaining( idx -> {
            if (idx >= shape.getNumberOfBits())
            {
                throw new IllegalArgumentException( String.format( "Bit index (%s) is too big", idx ));
            }
            if (idx < 0 ) {
                throw new IllegalArgumentException( String.format( "Bit index (%s) may not be less than zero", idx ));
            }
            set.add( idx );
        });
        values = new ArrayList<Integer>(set);

    }

    /**
     * Gets the shape this static hasher was created with.
     *
     * @return the Shape of this hasher.
     */
    public Shape getShape() {
        return shape;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns an iterator of integers that are the bits to enable in the Bloom
     * filter based on the shape.  The iterator will not return the same value multiple
     * times.  Values will be returned in ascending order.
     *
     * @param shape the shape of the desired Bloom filter.
     * @return the Iterator of integers;
     * @throws IllegalArgumentException if {@code shape.getHasherName()} does not
     *                                  equal {@code getName()}
     */
    @Override
    public OfInt getBits(Shape shape) {
        if (!this.shape.equals(shape)) {
            throw new IllegalArgumentException("shape does not match internal shape");
        }
        return new Iter();
    }

    /**
     * The iterator of integers.
     */
    private class Iter implements PrimitiveIterator.OfInt {
        private int idx = 0;

        @Override
        public boolean hasNext() {
            return idx < values.size();
        }

        @Override
        public int nextInt() {
            if (hasNext()) {
                return values.get(idx++);
            }
            throw new NoSuchElementException();
        }
    }
}
