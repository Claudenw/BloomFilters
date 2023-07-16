/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloomfilter.stable;

import java.util.BitSet;
import java.util.function.IntPredicate;

import org.apache.commons.collections4.bloomfilter.BitCountProducer.BitCountConsumer;

/**
 * An object that produces indices of a Bloom filter.
 * <p>
 * <em> The default implementation of {@code asIndexArray} is slow. Implementers
 * should reimplement the method where possible.</em>
 * </p>
 *
 * @since 4.5
 */
@FunctionalInterface
public interface CellProducer {

    /**
     * Each index with a value greater than zero is passed to the predicate. The
     * predicate is applied to each index value, if the predicate returns
     * {@code false} the execution is stopped, {@code false} is returned, and no
     * further indices are processed.
     *
     * <p>
     * Any exceptions thrown by the action are relayed to the caller.
     * </p>
     *
     * <p>
     * Indices ordering and uniqueness is not guaranteed.
     * </p>
     *
     * @param predicate the action to be performed for each non-zero bit index.
     * @return {@code true} if all cells return true from consumer, {@code false}
     * otherwise.
     * @throws NullPointerException if the specified predicate is null
     */
    default boolean forEachCell(IntPredicate predicate) {
        return forEachCell((idx, val) -> (val == 0) ? true : predicate.test(idx));
    }

    /**
     * For each cell in the producer call the consumer with the index and the value.
     * 
     * @param consumer the consumer for the index and value.
     * @return {@code true} if all cells return true from consumer, {@code false}
     * otherwise.
     */
    boolean forEachCell(BitCountConsumer consumer);

    /**
     * Creates an IndexProducer from an array of integers.
     * 
     * @param values the index values
     * @return an IndexProducer that uses the values.
     */
    static CellProducer fromIndexArray(final int... values) {
        return new CellProducer() {

            @Override
            public boolean forEachCell(final BitCountConsumer consumer) {
                for (int idx = 0; idx < values.length; idx++) {
                    if (!consumer.test(idx, values[idx])) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public int[] asIndexArray() {
                return values.clone();
            }
        };
    }

    /**
     * Return a copy of the IndexProducer data as an int array.
     *
     * <p>
     * Indices ordering and uniqueness is not guaranteed.
     * </p>
     *
     * <p>
     * <em> The default implementation of this method is slow. It is recommended
     * that implementing classes reimplement this method. </em>
     * </p>
     *
     * <p>
     * <em> The default implementation of this method returns unique values in
     * order. </em>
     * </p>
     * 
     * @return An int array of the data.
     */
    default int[] asIndexArray() {
        final BitSet result = new BitSet();
        forEachCell(i -> {
            result.set(i);
            return true;
        });
        return result.stream().toArray();
    }
}
