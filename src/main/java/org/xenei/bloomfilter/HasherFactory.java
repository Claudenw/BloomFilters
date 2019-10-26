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
 * The class that performs hashing.
 * <p>
 * Hashers are known by their implementation of the Hasher.Func interface.
 * <ul>
 * <li>Each Hasher.Func has a unique name, and is registered with the Hasher
 * class via the static method {@code Hasher.register( Func func )}.</li>
 * <li>Hashers are retrieved via the static method
 * {@code Hasher.getHasher( String name )}, where name is the well known name of
 * the Hasher.Func.</li>
 * <li>The name of all known Funcs can be listed by calling the static method
 * {@code Hasher.listFuncs()}.</li>
 * <p>
 * The Hasher is guaranteed to have the Funcs defiend in the the
 * {@code org.xenei.bloomfilter.hasher} package registered.
 *
 */
public class HasherFactory {

    private static final Map<String, Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>>> funcMap;

    static {
        funcMap = new HashMap<String, Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>>>();
        resetFuncs();
    }

    /**
     * Registers a Func implementation. After registration the Func name can be used
     * to retrieve a Hasher.
     * <p>
     *
     * The func calculates the long value that is used to turn on a bit in the Bloom
     * filter. The first argument is a {@code ByteBuffer} containing the bytes to be
     * indexed, the second argument is a seed index.
     * </p>
     * <p>
     * On the first call to {@code applyAsLong} the seed index will be 0 and the
     * func should start the hash sequence.
     * </p>
     * <p>
     * On subsequent calls the hash function using the same buffer the seed index
     * will be incremented. The func should return a different calculated value on
     * each call. The func may use the seed as part of the calculation or simply use
     * it to detect when the buffer has changed.
     * </p>
     *
     * @see #getHasher(String)
     * @param func the Func to register.
     * @throws SecurityException     if the no argument constructor can not be
     *                               accessed.
     * @throws NoSuchMethodException if func does not have a no argument
     *                               constructor.
     */
    public static void register(String name, Class<? extends ToLongBiFunction<ByteBuffer, Integer>> func)
            throws NoSuchMethodException, SecurityException {
        Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>> c = func.getConstructor();
        funcMap.put(name, c);
    }

    /**
     * Lists all registered Funcs.
     *
     * @return the list of all registered Func names.
     */
    public static Set<String> listFuncs() {
        return Collections.unmodifiableSet(funcMap.keySet());
    }

    /**
     * Reset registered Funcs to initial known set.
     *
     */
    public static void resetFuncs() {
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
     * @param funcName the name of the hasher to create.
     * @return the Hasher
     * @throws IllegalArgumentException if the funcName is not registered.
     * @throws IllegalStateException    if the function can not be instantiated.
     */
    public static DynamicHasher getHasher(String funcName) throws IllegalArgumentException {
        Constructor<? extends ToLongBiFunction<ByteBuffer, Integer>> c = funcMap.get(funcName);
        if (c == null) {
            throw new IllegalArgumentException("No function implementation named " + funcName);
        }
        try {
            return new DynamicHasher(funcName, c.newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Unable to call constructor for " + funcName, e);
        }
    }

    public interface Hasher {

        /**
         * Gets the name of the function.
         *
         * @return the name of the function being used.
         */
        public String getName();

        /**
         * Return an iterator of integers that are the bits to enable in the Bloom
         * filter based on the shape.
         * <p>
         * Once this method is called the Hasher is locked and no further properties may
         * be added.
         * </p>
         *
         * @param shape the shape of the desired Bloom filter.
         * @return the interator of integers;
         * @throws IllegalArgumentException if {@code shape,getHasherName()} does not
         *                                  equal {@code func.getName()}
         */
        public PrimitiveIterator.OfInt getBits(Shape shape);
    }

}
