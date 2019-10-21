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

import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.xenei.bloomfilter.hasher.Hasher;

/**
 * A counting Bloom filter.
 *
 * <p> This Bloom filter maintains a count of the number of times a bit has been turned
 * on. This allows for removal of Bloom filters from the filter. </p> <p> Instances are
 * immutable. </p>
 *
 * @since 4.5
 */
public class CountingBloomFilter implements BloomFilter {


    /**
     * the count of entries. Each enabled bit is a key with the count for that bit being
     * the value.
     */
    private final TreeMap<Integer, Integer> counts;

    private final Shape shape;

    /**
     * Constructor.
     *
     * @param protoFilter the protoFilter to build this Bloom filter from.
     * @param config the Filter configuration to use to build the Bloom filter.
     */
    public CountingBloomFilter(Hasher hasher, Shape shape) {
        if (!hasher.getName().equals( shape.getHasherName() ))
        {
            throw new IllegalArgumentException(
                    String.format("Hasher names do not match %s != %s", hasher.getName(), shape.getHasherName()));
        }

        this.shape = shape;

        counts = new TreeMap<Integer, Integer>();
        Set<Integer> idxs = new HashSet<Integer>();
        OfInt iter = hasher.getBits(shape);
        while (iter.hasNext())
        {
            idxs.add( iter.next() );
        }
        idxs.parallelStream().forEach( idx -> counts.put(idx, 1));
    }


    /**
     * Constructor.
     *
     * Construct a CountingBloomFilter from a map of enabledBits and the count of the
     * number of times the bit was enabled.
     *
     * @param counts the Map of set bits to counts for that bit.
     */
    public CountingBloomFilter(Shape shape) {
        this( new TreeMap<Integer, Integer>(), shape );
    }


    /**
     * Constructor.
     *
     * Construct a CountingBloomFilter from a map of enabledBits and the count of the
     * number of times the bit was enabled.
     *
     * @param counts the Map of set bits to counts for that bit.
     */
    public CountingBloomFilter(Map<Integer, Integer> counts, Shape shape) {
        this.counts = new TreeMap<Integer, Integer>(counts);
        this.shape = shape;
    }

    /**
     * Gets the count for each enabled bit.
     *
     * @return an immutable map of enabled bits (key) to counts for that bit (value).
     */
    public Stream<Map.Entry<Integer, Integer>> getCounts() {
        return counts.entrySet().stream();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{ ");
        for (Map.Entry<Integer, Integer> e : counts.entrySet()) {
            sb.append(String.format("(%s,%s) ", e.getKey(), e.getValue()));
        }
        return sb.append("}").toString();
    }

    /**
     * Merge this Bloom filter with the other creating a new filter. The counts for bits
     * that are on in the other filter are incremented. <p> For each bit that is turned on
     * in the other filter; if the other filter is also a CountingBloomFilter the count is
     * added to this filter, otherwise the count is incremented by one. </p>
     *
     * @param other the other filter.
     * @return a new filter.
     */
    @Override
    public void merge(BloomFilter other) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        merge( BitSet.valueOf( other.getBits() ).stream().parallel().boxed());
    }

    public void merge(CountingBloomFilter other) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        merge( other.counts.keySet().parallelStream());
    }

    private void merge( Stream<Integer> idxStream )
    {
        idxStream.forEach( idx -> counts.put(idx, counts.get(idx)+1 ) );
    }

    /**
     * Decrement the counts for the bits that are on in the other BloomFilter from this
     * one.
     *
     * <p> For each bit that is turned on in the other filter; if the other filter is also
     * a CountingBloomFilter the count is subtracted from this filter, otherwise the count
     * is decremented by 1. </p>
     *
     * @param other the other filter.
     * @return a new filter.
     */
    public void remove(BloomFilter other) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        remove( BitSet.valueOf( other.getBits() ).stream().parallel().boxed());
    }

    public void remove(CountingBloomFilter other) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        remove( other.counts.keySet().parallelStream());
    }

    private void remove( Stream<Integer> idxStream )
    {
        idxStream.forEach( idx -> counts.put(idx, counts.get(idx)-1 ) );
    }


    @Override
    public LongBuffer getBits() {
        BitSet bs = new BitSet();
        counts.keySet().parallelStream().forEach( bs::set );
        return LongBuffer.wrap(bs.toLongArray());
    }


    @Override
    public Shape getShape() {
       return shape;
    }

}
