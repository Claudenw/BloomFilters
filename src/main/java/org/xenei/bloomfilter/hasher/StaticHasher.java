package org.xenei.bloomfilter.hasher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.function.Consumer;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.HasherFactory.Hasher;

/**
 * A Hasher implementation that contains all the bits for a specific shape of a
 * DynamicHasher.
 *
 */
public class StaticHasher implements Hasher {

    private final String name;
    private final Shape shape;
    private final List<Integer> values;

    public StaticHasher(DynamicHasher other, Shape shape) {
        this.name = other.getName();
        this.shape = shape;
        Set<Integer> set = new HashSet<Integer>();
        other.getBits(shape).forEachRemaining((Consumer<Integer>) set::add);
        values = new ArrayList<Integer>(set);
    }

    public StaticHasher(Iterator<Integer> iter, Shape shape) {
        this.name = shape.getHasherName();
        this.shape = shape;
        Set<Integer> set = new HashSet<Integer>();
        iter.forEachRemaining((Consumer<Integer>) set::add);
        values = new ArrayList<Integer>(set);
    }

    /**
     * Gets the shape this static hasher was created with.
     * 
     * @return the Shape of this hashers values.
     */
    public Shape getShape() {
        return shape;
    }

    @Override
    public String getName() {
        return name;
    }

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
