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
 * A bloom filter.
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
        super( shape );
        if (!hasher.getName().equals( shape.getHasherName() ))
        {
            throw new IllegalArgumentException(
                    String.format("Hasher names do not match %s != %s", hasher.getName(), shape.getHasherName()));
        }
        this.bitSet = new BitSet();
        OfInt iter = hasher.getBits(shape);
        while (iter.hasNext())
        {
            bitSet.set( iter.nextInt() );
        }
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
     * Merge this bloom filter with the other creating a new filter.
     *
     * @param other the other filter.
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


    public int andCardinality(BitSetBloomFilter other) {
        verifyShape( other );
        BitSet result = (BitSet) bitSet.clone();
        result.and( other.bitSet );
        return result.cardinality();
    }


    public int orCardinality(BitSetBloomFilter other) {
        verifyShape( other );
        BitSet result = (BitSet) bitSet.clone();
        result.or( other.bitSet );
        return result.cardinality();
    }

    public int xorCardinality(BitSetBloomFilter other) {
        verifyShape( other );
        BitSet result = (BitSet) bitSet.clone();
        result.xor( other.bitSet );
        return result.cardinality();
    }

}
