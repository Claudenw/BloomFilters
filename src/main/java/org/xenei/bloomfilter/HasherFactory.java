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
package org.xenei.bloomfilter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.ToLongBiFunction;

import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.hasher.DynamicHasher;
import org.xenei.bloomfilter.hasher.MD5;
import org.xenei.bloomfilter.hasher.Murmur128;
import org.xenei.bloomfilter.hasher.Murmur32;
import org.xenei.bloomfilter.hasher.ObjectsHash;

/**
 * A factory that produces Hashers.
 *
 * <ul>
 * <li>A Hasher may be registered with the HasherFactory
 * class via the static method {@code HasherFactory.register( String, Class<? extends ToLongBiFunction<ByteBuffer, Integer>> )}.</li>
 * <li>Hashers may be retrieved via the static method
 * {@code HasherFactory.getHasher( String name )}, where name is the name that was provided
 * during the {@code HasherFactory.register()} call.</li>
 * <li>The names of all registered Hashers can be listed by calling the static method
 * {@code HasherFactory.listHashers()}.</li>
 * </ul>
 * <p>
 * The Hasher is guaranteed to have the Hashers defiend in the the
 * {@code org.xenei.bloomfilter.hasher} package registered.
 *
 */
public class HasherFactory {

    private static final Map<String, Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>>> funcMap;

    static {
        funcMap = new HashMap<String, Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>>>();
        reset();
    }

    /**
     * Registers a Hasher implementation. After registration the name can be used
     * to retrieve the Hasher.
     * <p>
     * The function calculates the long value that is used to turn on a bit in the Bloom
     * filter. The first argument is a {@code ByteBuffer} containing the bytes to be
     * indexed, the second argument is a seed index.
     * </p>
     * <p>
     * On the first call to {@code applyAsLong} the seed index will be 0 and the
     * function should start the hash sequence.
     * </p>
     * <p>
     * On subsequent calls the hash function using the same buffer the seed index
     * will be incremented. The function should return a different calculated value on
     * each call. The function may use the seed as part of the calculation or simply use
     * it to detect when the buffer has changed.
     * </p>
     *
     * @see #getHasher(String)
     * @param name The name of the hasher
     * @param functionClass The function for the hasher to use.  Must have a zero argument constructor.
     * @throws SecurityException     if the no argument constructor can not be
     *                               accessed.
     * @throws NoSuchMethodException if functionClass does not have a no argument
     *                               constructor.
     */
    public static void register(String name, Class<? extends ToLongBiFunction<ByteBuffer, Integer>> functionClass)
            throws NoSuchMethodException, SecurityException {
        Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>> c = functionClass.getConstructor();
        funcMap.put(name, c);
    }

    /**
     * Lists all registered Hashers.
     *
     * @return the list of all registered Hasher names.
     */
    public static Set<String> listFuncs() {
        return Collections.unmodifiableSet(funcMap.keySet());
    }

    /**
     * Reset registered Hahsers to initial known set.
     *
     */
    public static void reset() {
        funcMap.clear();
        try {
            register(MD5.name, MD5.class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Can not get MD5 constructor");
        }
        try {
            register(Murmur128.name, Murmur128.class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Can not get Murmur128 constructor");
        }
        try {
            register(Murmur32.name, Murmur128.class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Can not get Murmur128 constructor");
        }
        try {
            register(ObjectsHash.name, ObjectsHash.class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Can not get ObjectsHash constructor");

        }

    }

    /**
     * Gets the specified hasher.
     *
     * @param name the name of the hasher to create.
     * @return A DynamicHasher of the specified type.
     * @throws IllegalArgumentException if the funcName is not registered.
     * @throws IllegalStateException    if the function can not be instantiated.
     */
    public static DynamicHasher getHasher(String name) throws IllegalArgumentException {
        Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>> c = funcMap.get(name);
        if (c == null) {
            throw new IllegalArgumentException("No function implementation named " + name);
        }
        try {
            return new DynamicHasher(name, c.newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Unable to call constructor for " + name, e);
        }
    }

    /**
     * The class that performs hashing.
     * <p>
     * Hashers have a Unique name.
     * <ul>
     * <li>A Hasher may be registered with the HasherFactory
     * class via the static method {@code HasherFactory.register( String, Class<? extends ToLongBiFunction<ByteBuffer, Integer>> )}.</li>
     * <li>Hashers may be retrieved via the static method
     * {@code HasherFactory.getHasher( String name )}, where name is the name that was provided
     * during the {@code HasherFactory.register()} call.</li>
     * </ul>
     * <p>
     * Implementations of {@code getBits()} may return duplicate values and may return
     * values in a random order.  See implementation javadoc notes as to the guarantees
     * provided by the specific implementation.
     */
    public interface Hasher {

        /**
         * Gets the name of the function.
         *
         * @return the name of the function being used.
         */
        public String getName();

        /**
         * Return an iterator of integers that are the bits to enable in the Bloom
         * filter based on the shape.  No guarantee is made as to order
         * or duplication of values.
         *
         * @param shape the shape of the desired Bloom filter.
         * @return the Iterator of integers;
         * @throws IllegalArgumentException if {@code shape.getHasherName()} does not
         *                                  equal {@code getName()}
         */
        public PrimitiveIterator.OfInt getBits(Shape shape);
    }

}
