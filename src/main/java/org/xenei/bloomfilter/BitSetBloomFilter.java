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



/**
 * A bloom filter using a Java BitSet to track enabled bits.
 * This is a standard implementation and should work well for most
 * Bloom filters.
 *
 */
public class BitSetBloomFilter extends BloomFilter {

    /**
     * The bitset that defines this BloomFilter.
     */
    private BitSet bitSet;


    @Override
    public LongBuffer getBits() {
        return LongBuffer.wrap( bitSet.toLongArray() );
    }

    @Override
    public StaticHasher getHasher() {
        return new StaticHasher(bitSet.stream().iterator(), getShape());
    }

    public BitSetBloomFilter(Hasher hasher, Shape shape) {
        this( shape );
        verifyHasher(hasher);
        hasher.getBits(shape).forEachRemaining((IntConsumer) bitSet::set);
    }

    /**
     * Constructor.
     *
     * @param shape The BloomFilter.Shape to define this BloomFilter.
     */
    public BitSetBloomFilter(Shape shape) {
        super( shape );
        this.bitSet = new BitSet();
    }


    @Override
    public void merge(BloomFilter other) {
        verifyShape( other );
        bitSet.or( BitSet.valueOf( other.getBits() ));
    }

    @Override
    public boolean contains( Shape shape, Hasher hasher ) {
        verifyShape( shape );
        if ( ! shape.getHasherName().equals( hasher.getName() ))
        {
            throw new IllegalArgumentException( String.format("Hasher (%s) is not the sames as for shape (%s)", hasher.getName(), shape.getHasherName()));
        }
        OfInt iter = hasher.getBits(shape);
        while (iter.hasNext())
        {
            if (!bitSet.get(iter.nextInt()))
            {
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
     * Merge another BitSetBloomFilter into this one.
     * <p>
     * This mehtod takes advantage of internal structures of
     * BitSetBloomFilter.
     * </p>
     *
     * @param other the other BitSetBloomFilter.
     * @see #merge(BloomFilter)
     */
    public void merge(BitSetBloomFilter other) {
        verifyShape( other );
        bitSet.or( other.bitSet );
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


    /**
     * Calculates the andCardinality with another BitSetBloomFilter.
     * <p>
     * This method takes advantage of internal structures of
     * BitSetBloomFilter.
     * </p>
     *
     * @param other the other BitSetBloomFilter.
     * @see #andCardinality(BloomFilter)
     */
    public int andCardinality(BitSetBloomFilter other) {
        verifyShape( other );
        BitSet result = (BitSet) bitSet.clone();
        result.and( other.bitSet );
        return result.cardinality();
    }


    /**
     * Calculates the orCardinality with another BitSetBloomFilter.
     * <p>
     * This method takes advantage of internal structures of
     * BitSetBloomFilter.
     * </p>
     *
     * @param other the other BitSetBloomFilter.
     * @see #orCardinality(BloomFilter)
     */
    public int orCardinality(BitSetBloomFilter other) {
        verifyShape( other );
        BitSet result = (BitSet) bitSet.clone();
        result.or( other.bitSet );
        return result.cardinality();
    }

    /**
     * Calculates the xorCardinality with another BitSetBloomFilter.
     * <p>
     * This method takes advantage of internal structures of
     * BitSetBloomFilter.
     * </p>
     *
     * @param other the other BitSetBloomFilter.
     * @see #xorCardinality(BloomFilter)
     */
    public int xorCardinality(BitSetBloomFilter other) {
        verifyShape( other );
        BitSet result = (BitSet) bitSet.clone();
        result.xor( other.bitSet );
        return result.cardinality();
    }

}
