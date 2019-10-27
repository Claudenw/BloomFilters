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
import java.util.BitSet;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.IntConsumer;

import org.xenei.bloomfilter.HasherFactory.Hasher;
import org.xenei.bloomfilter.hasher.StaticHasher;

import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * A bloom filter that uses EWAH compressed bitmaps to store enabled bits.
 * This filter is a good choice for large filters (high m value) with
 * a relatively low number of functions (k value).
 *
 */
public class EWAHBloomFilter extends BloomFilter {

    /**
     * The bitset that defines this BloomFilter.
     */
    private EWAHCompressedBitmap bitSet;

    /**
     * Constructs a filter from a hasher and shape.
     * @param hasher the hasher to use
     * @param shape the shape.
     */
    public EWAHBloomFilter(Hasher hasher, Shape shape) {
        this(shape);
        verifyHasher(hasher);
        hasher.getBits(shape).forEachRemaining((IntConsumer) bitSet::set);
    }

    /**
     * Constructors an empty filter with the prescribed shape.
     *
     * @param shape The BloomFilter.Shape to define this BloomFilter.
     */
    public EWAHBloomFilter(Shape shape) {
        super(shape);
        this.bitSet = new EWAHCompressedBitmap();
    }

    @Override
    public StaticHasher getHasher() {
        return new StaticHasher(bitSet.iterator(), getShape());
    }

    @Override
    public LongBuffer getBits() {
        BitSet bs = new BitSet();
        bitSet.forEach(bs::set);
        return LongBuffer.wrap(bs.toLongArray());
    }


    @Override
    public void merge(BloomFilter other) {
        verifyShape(other);
        bitSet = bitSet.or(new EWAHCompressedBitmap(other.getBits()));
    }

    @Override
    public void merge( Shape shape, Hasher hasher ) {
        verifyShape( shape );
        if ( ! shape.getHasherName().equals( hasher.getName() ))
        {
            throw new IllegalArgumentException( String.format("Hasher (%s) is not the sames as for shape (%s)", hasher.getName(), shape.getHasherName()));
        }
        hasher.getBits(shape).forEachRemaining((IntConsumer) bitSet::set );
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
            if (!bitSet.get(iter.nextInt())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hammingValue() {
        return bitSet.cardinality();
    }

    @Override
    public String toString() {
        return bitSet.toString();
    }

    /**
     * Merge an EWAHBloomFilter into this one.
     * <p>
     * This method takes advantage of the internal structure
     * of the EWAHBloomFilter.
     * </p>
     *
     * @param other the other EWAHBloomFilter filter.
     */
    public void merge(EWAHBloomFilter other) {
        verifyShape(other);
        bitSet = bitSet.or(other.bitSet);
    }

    /**
     * Calculate the andCardinality with another EWAHBloomFilter.
     * <p>
     * This method takes advantage of the internal structure
     * of the EWAHBloomFilter.
     * </p>
     *
     * @param other the other EWAHBloomFilter filter.
     * @see #andCardinality(BloomFilter)
     */
    public int andCardinality(EWAHBloomFilter other) {
        verifyShape(other);
        return bitSet.andCardinality(other.bitSet);
    }

    /**
     * Calculate the orCardinality with another EWAHBloomFilter.
     * <p>
     * This method takes advantage of the internal structure
     * of the EWAHBloomFilter.
     * </p>
     *
     * @param other the other EWAHBloomFilter filter.
     * @see #orCardinality(BloomFilter)
     */
    public int orCardinality(EWAHBloomFilter other) {
        verifyShape(other);
        return bitSet.orCardinality(other.bitSet);
    }

    /**
     * Calculate the xorCardinality with another EWAHBloomFilter.
     * <p>
     * This method takes advantage of the internal structure
     * of the EWAHBloomFilter.
     * </p>
     *
     * @param other the other EWAHBloomFilter filter.
     * @see #xorCardinality(BloomFilter)
     */
    public int xorCardinality(EWAHBloomFilter other) {
        verifyShape(other);
        return bitSet.xorCardinality(other.bitSet);
    }

}
