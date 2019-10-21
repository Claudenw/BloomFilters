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

import org.xenei.bloomfilter.hasher.Hasher;


/**
 * A bloom filter.
 *
 */
public class BloomFilterImpl implements BloomFilter {

    /**
     * The bitset that defines this BloomFilter.
     */
    private final BitSet bitSet;

    /**
     * The shape of this BloomFilter.
     */
    private final Shape shape;

	@Override
    public LongBuffer getBits() {
	    return LongBuffer.wrap( bitSet.toLongArray() ).asReadOnlyBuffer();
	}

	@Override
	public Shape getShape() {
	    return shape;
	}

	public BloomFilterImpl(Hasher hasher, Shape shape) {
	    if (!hasher.getName().equals( shape.getHasherName() ))
        {
            throw new IllegalArgumentException(
                    String.format("Hasher names do not match %s != %s", hasher.getName(), shape.getHasherName()));
        }
	    this.bitSet = new BitSet();
	    this.shape = shape;
	    OfInt iter = hasher.getBits(shape);
	    while (iter.hasNext())
	    {
	        bitSet.set( iter.nextInt() );
	    }
	}

	/**
	 * Constructor
	 *
	 * @param bitSet The bit set that was built by the config.
	 */
	public BloomFilterImpl(BitSet bitSet, Shape shape) {
		this.bitSet = bitSet;
		this.shape = shape;
	}

	/**
	 * Constructor.
	 *
	 * @param shape The BloomFilter.Shape to define this BloomFilter.
	 */
	public BloomFilterImpl(Shape shape) {
		this(new BitSet(shape.getNumberOfBits()), shape);
	}


	@Override
    public void merge(BloomFilter other) {
	    shape.verifyHasher( other.getShape() );
        bitSet.or( BitSet.valueOf( other.getBits() ));
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
	 * @return a new filter.
	 */
	public void merge(BloomFilterImpl other) {
	    shape.verifyHasher( other );
	    bitSet.or( other.bitSet );
	}
}
