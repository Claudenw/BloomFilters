package org.xenei.bloomfilter.hasher;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.TreeSet;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.HasherFactory.Hasher;

/**
 * A Hasher implementation that contains the index for all enabled bits for a specific shape.
 *
 */
public final class StaticHasher implements Hasher {

    /**
     * The shape of this hasher
     */
    private final Shape shape;
    /**
     * The ordered set of values that this hasher will return.
     */
    private final Set<Integer> values;

    /**
     * Constructs the StaticHasher from a DynamicHasher and a Shape.
     * @param hasher the DynamicHasher to read.
     * @param shape the Shape for the resulting values.
     */
    public StaticHasher(Hasher hasher, Shape shape) {
        this( hasher.getBits(shape), shape);
    }

    /**
     * Constructs a StaticHasher from an Iterator of Integers and a Shape.
     * @param iter the Iterator of Integers.
     * @param shape the Shape that the integers were generated for.
     * @throws IllegalArgumentException if any Integer is outside the range [0,shape.getNumberOfBits())
     */
    public StaticHasher(Iterator<Integer> iter, Shape shape) {
        this.shape = shape;
        this.values = new TreeSet<Integer>();
        iter.forEachRemaining( idx -> {
            if (idx >= shape.getNumberOfBits())
            {
                throw new IllegalArgumentException( String.format( "Bit index (%s) is too big", idx ));
            }
            if (idx < 0 ) {
                throw new IllegalArgumentException( String.format( "Bit index (%s) may not be less than zero", idx ));
            }
            values.add( idx );
        });
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
        return shape.getHasherName();
    }

    /**
     * Gets the the number of unique values in this hasher.
     * @return the number of unique values.
     */
    public int size() {
        return values.size();
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
        return new Iter(values.iterator());
    }

    public class Iter implements PrimitiveIterator.OfInt, Iterator<Integer> {

        Iterator<Integer> wrapped;

        Iter(Iterator<Integer> wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public boolean hasNext() {
            return wrapped.hasNext();
        }

        @Override
        public int nextInt() {
            return wrapped.next();
        }

        @Override
        public Integer next() {
            return wrapped.next();
        }



    }
}
