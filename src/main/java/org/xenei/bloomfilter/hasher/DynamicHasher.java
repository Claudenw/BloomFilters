/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.ToLongBiFunction;

import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.HasherFactory.Hasher;

/**
 * The class that performs hashing on demand.  Items can be added to the hasher
 * using the {@code with()} methods.  once {@code getBits()} method is called it
 * is an error to call {@code with()} again.
 */
public class DynamicHasher implements Hasher {

    /**
     * The list of ByteBuffers that are to be hashed.
     */
    private final List<ByteBuffer> buffers;

    /**
     * The function to hash the buffers.
     */
    private final ToLongBiFunction<ByteBuffer, Integer> function;

    /**
     * The name of the function.
     */
    private final String name;

    /**
     * True if the hasher is locked.
     */
    private boolean locked;

    /**
     * The constructor
     *
     * @param function the function to use.
     */
    public DynamicHasher(String name, ToLongBiFunction<ByteBuffer, Integer> function) {
        this.buffers = new ArrayList<ByteBuffer>();
        this.function = function;
        this.name = name;
        this.locked = false;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Return an iterator of integers that are the bits to enable in the Bloom
     * filter based on the shape.  The iterator may return the same value multiple
     * times.  There is no guarantee made as to the order of the integers.
     * <p>
     * Once this method is called the Hasher is locked and no further properties may
     * be added.
     * </p>
     *
     * @param shape the shape of the desired Bloom filter.
     * @return the Iterator of integers;
     * @throws IllegalArgumentException if {@code shape.getHasherName()} does not
     *                                  equal {@code getName()}
     */
    @Override
    public PrimitiveIterator.OfInt getBits(Shape shape) {
        if (!getName().equals(shape.getHasherName())) {
            throw new IllegalArgumentException(
                    String.format("Shape hasher %s is not %s", shape.getHasherName(), getName()));
        }
        locked = true;
        return new Iter(shape);
    }

    /**
     * Adds a ByteBuffer to the hasher.
     *
     * @param property the ByteBuffer to add.
     * @returns {@code this} for chaining.
     * @throws IllegalStateException if the Hasher is locked.
     * @see #getBits(Shape)
     */
    public final DynamicHasher with(ByteBuffer property) {
        if (locked) {
            throw new IllegalStateException("Attempted to add to a locked Hasher");
        }
        buffers.add(property);
        return this;
    }

    /**
     * Adds a byte to the hasher.
     *
     * @param property the byte to add
     * @returns {@code this} for chaining.
     * @throws IllegalStateException if the Hasher is locked.
     * @see #getBits(Shape)
     */
    public final DynamicHasher with(byte property) {
        return with(ByteBuffer.wrap(new byte[] { property }));
    }

    /**
     * Adds an array of bytes to the hasher.
     *
     * @param property the array of bytes to add.
     * @returns {@code this} for chaining.
     * @throws IllegalStateException if the Hasher is locked.
     * @see #getBits(Shape)
     */
    public final DynamicHasher with(byte[] property) {
        return with(ByteBuffer.wrap(property));
    }

    /**
     * Adds a string to the hasher. The string is converted to a byte array using
     * the UTF-8 Character set.
     *
     * @param property the string to add.
     * @throws IllegalStateException if the Hasher is locked.
     * @see #getBits(Shape)
     */
    public final DynamicHasher with(String property) {
        return with(property.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The iterator of integers.
     */
    private class Iter implements PrimitiveIterator.OfInt {
        private int buffer = 0;
        private int funcCount = 0;
        private final Shape shape;

        /**
         * Creates iterator with the specified shape.
         *
         * @param shape
         */
        private Iter(Shape shape) {
            this.shape = shape;
        }

        @Override
        public boolean hasNext() {
            return buffer < buffers.size() - 1 || funcCount < shape.getNumberOfHashFunctions();
        }

        @Override
        public int nextInt() {
            if (hasNext()) {
                if (funcCount >= shape.getNumberOfHashFunctions()) {
                    funcCount = 0;
                    buffer++;
                }
                return Math.floorMod(function.applyAsLong(buffers.get(buffer), funcCount++), shape.getNumberOfBits());
            }
            throw new NoSuchElementException();
        }
    }

}
