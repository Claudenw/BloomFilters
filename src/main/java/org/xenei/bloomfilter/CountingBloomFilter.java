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
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.xenei.bloomfilter.HasherFactory.Hasher;
import org.xenei.bloomfilter.hasher.StaticHasher;

/**
 * A counting Bloom filter.
 *
 * <p>
 * This Bloom filter maintains a count of the number of times a bit has been
 * turned on. This allows for removal of Bloom filters from the filter.
 * </p>
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @since 4.5
 */
public class CountingBloomFilter extends BloomFilter {

    /**
     * the count of entries. Each enabled bit is a key with the count for that bit
     * being the value.
     */
    private final TreeMap<Integer, Integer> counts;

    /**
     * Constructor.
     *
     * @param protoFilter the protoFilter to build this Bloom filter from.
     * @param config      the Filter configuration to use to build the Bloom filter.
     */
    public CountingBloomFilter(Hasher hasher, Shape shape) {
        super(shape);
        if (!hasher.getName().equals(shape.getHasherName())) {
            throw new IllegalArgumentException(
                    String.format("Hasher names do not match %s != %s", hasher.getName(), shape.getHasherName()));
        }

        counts = new TreeMap<Integer, Integer>();
        Set<Integer> idxs = new HashSet<Integer>();
        OfInt iter = hasher.getBits(shape);
        while (iter.hasNext()) {
            idxs.add(iter.next());
        }
        idxs.stream().forEach(idx -> counts.put(idx, 1));
    }

    /**
     * Constructor.
     *
     * Construct a CountingBloomFilter from a map of enabledBits and the count of
     * the number of times the bit was enabled.
     *
     * @param counts the Map of set bits to counts for that bit.
     */
    public CountingBloomFilter(Shape shape) {
        this(new TreeMap<Integer, Integer>(), shape);
    }

    /**
     * Constructor.
     *
     * Construct a CountingBloomFilter from a map of enabledBits and the count of
     * the number of times the bit was enabled.
     *
     * @param counts the Map of set bits to counts for that bit.
     */
    public CountingBloomFilter(Map<Integer, Integer> counts, Shape shape) {
        super(shape);
        this.counts = new TreeMap<Integer, Integer>(counts);
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
        merge(BitSet.valueOf(other.getBits()).stream().boxed());
    }

    public void merge(CountingBloomFilter other) {
        verifyShape(other);
        merge(other.counts.keySet().stream());
    }

    private void merge(Stream<Integer> idxStream) {
        idxStream.forEach(idx -> {
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

    public void remove(CountingBloomFilter other) {
        verifyShape(other);
        remove(other.counts.keySet().parallelStream());
    }

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
}
