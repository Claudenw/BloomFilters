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
import java.util.AbstractMap;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.IntConsumer;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.xenei.bloomfilter.HasherFactory.Hasher;
import org.xenei.bloomfilter.hasher.StaticHasher;

/**
 * A counting Bloom filter.
 * This Bloom filter maintains a count of the number of times a bit has been
 * turned on. This allows for removal of Bloom filters from the filter.
 * <p>
 * This implementation uses a map to track enabled bit counts
 * </p>
 *
 */
public class CountingBloomFilter extends BloomFilter {

    /**
     * the count of entries. Each enabled bit is a key with the count for that bit
     * being the value.  Entries with a value of zero are removed.
     */
    private final TreeMap<Integer, Integer> counts;

    /**
     * Constructs a counting Bloom filter from a hasher and a shape.
     *
     * @param protoFilter the protoFilter to build this Bloom filter from.
     * @param config      the Filter configuration to use to build the Bloom filter.
     */
    public CountingBloomFilter(Hasher hasher, Shape shape) {
        super(shape);
        verifyHasher(hasher);
        counts = new TreeMap<Integer, Integer>();
        Set<Integer> idxs = new HashSet<Integer>();
        hasher.getBits(shape).forEachRemaining((IntConsumer) idxs::add);
        idxs.stream().forEach(idx -> counts.put(idx, 1));
    }

    /**
     * Constructs an empty Counting filter with the specified shape.
     *
     * @param counts the Map of set bits to counts for that bit.
     */
    public CountingBloomFilter(Shape shape) {
        super(shape);
        this.counts = new TreeMap<Integer, Integer>();
    }

    /**
     * Gets the count for each enabled bit.
     *
     * @return an immutable map of enabled bits (key) to counts for that bit
     *         (value).
     */
    public Stream<Map.Entry<Integer, Integer>> getCounts() {
        return counts.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<Integer, Integer>(e.getKey(), e.getValue()));
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
     * Merge this Bloom filter with the other creating a new filter. The counts for
     * bits that are on in the other filter are incremented.
     * <p>
     * For each bit that is turned on in the other filter; if the other filter is
     * also a CountingBloomFilter the count is added to this filter, otherwise the
     * count is incremented by one.
     * </p>
     *
     * @param other the other filter.
     * @return a new filter.
     */
    @Override
    public void merge(BloomFilter other) {
        verifyShape(other);
        merge(BitSet.valueOf(other.getBits()).stream().iterator());
    }

    @Override
    public void merge( Shape shape, Hasher hasher ) {
        verifyShape( shape );
        if ( ! shape.getHasherName().equals( hasher.getName() ))
        {
            throw new IllegalArgumentException( String.format("Hasher (%s) is not the sames as for shape (%s)", hasher.getName(), shape.getHasherName()));
        }
        merge( hasher.getBits(shape) );
    }

    /**
     * Merge another CountingBloomFilter into this one. Takes advantage
     * of the internal structure of the CountingBloomFilter.
     * @param other the other CountingBloomFilter.
     */
    public void merge(CountingBloomFilter other) {
        verifyShape(other);
        merge(other.counts.keySet().iterator());
    }

    /**
     * Merge an iterator of set bits into this filter.
     * @param iter the iterator of bits to set.
     */
    private void merge(Iterator<Integer> iter) {
        iter.forEachRemaining(idx -> {
            Integer val = counts.get(idx);
            counts.put(idx, val == null ? 1 : val + 1);
        });
    }

    /**
     * Decrement the counts for the bits that are on in the other BloomFilter from
     * this one.
     *
     * <p>
     * For each bit that is turned on in the other filter; if the other filter is
     * also a CountingBloomFilter the count is subtracted from this filter,
     * otherwise the count is decremented by 1.
     * </p>
     *
     * @param other the other filter.
     * @return a new filter.
     */
    public void remove(BloomFilter other) {
        verifyShape(other);
        remove(BitSet.valueOf(other.getBits()).stream().parallel().boxed());
    }

    /**
     * Remove another CountingBloomFilter into this one. Takes advantage
     * of the internal structure of the CountingBloomFilter.
     * @param other the other CountingBloomFilter.
     */
    public void remove(CountingBloomFilter other) {
        verifyShape(other);
        remove(other.counts.keySet().stream());
    }

    /**
     * Decrements the counts for the bits specified in the Integer stream.
     *
     * @param idxStream The stream of bit counts to decrement.
     */
    private void remove(Stream<Integer> idxStream) {
        idxStream.forEach(idx -> {
            Integer val = counts.get(idx);
            if (val != null) {
                if (val - 1 == 0) {
                    counts.remove(idx);
                } else {
                    counts.put(idx, val - 1);
                }
            }
        });
    }

    @Override
    public LongBuffer getBits() {
        BitSet bs = new BitSet();
        counts.keySet().stream().forEach(bs::set);
        return LongBuffer.wrap(bs.toLongArray());
    }

    @Override
    public StaticHasher getHasher() {
        return new StaticHasher(counts.keySet().iterator(), getShape());
    }

    @Override
    public boolean contains(Shape shape, Hasher hasher) {
        verifyShape(shape);
        if (!shape.getHasherName().equals(hasher.getName())) {
            throw new IllegalArgumentException(String.format("Hasher (%s) is not the sames as for shape (%s)",
                    hasher.getName(), shape.getHasherName()));
        }
        OfInt iter = hasher.getBits(shape);
        while (iter.hasNext()) {
            if (counts.get(iter.nextInt()) == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hammingValue() {
        return counts.size();
    }

    /**
     * Calculates the orCardinality with another CountingBloomFilter.
     * @param other The other CountingBloomFilter
     * @return the orCardinality
     * @see #orCardinality(BloomFilter)
     */
    public int orCardinality(CountingBloomFilter other) {
        Set<Integer> result =
                new HashSet<Integer>( counts.keySet());
        result.addAll( other.counts.keySet() );
        return result.size();
    }

    /**
     * Calculates the andCardinality with another CountingBloomFilter.
     * @param other The other CountingBloomFilter
     * @return the andCardinality
     * @see #andCardinality(BloomFilter)
     */
    public int andCardinality(CountingBloomFilter other) {
        Set<Integer> result =
                new HashSet<Integer>( counts.keySet());
        result.retainAll( other.counts.keySet() );
        return result.size();
    }
}
