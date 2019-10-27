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
import java.util.Set;
import java.util.TreeSet;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.IntConsumer;

import org.apache.commons.collections4.iterators.EmptyIterator;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.xenei.bloomfilter.HasherFactory.Hasher;
import org.xenei.bloomfilter.hasher.StaticHasher;

/**
 * A Bloom filter built on a single hasher.
 * This filter type should only be used for small filters (few on bits)
 */
public class HasherBloomFilter extends BloomFilter {

    /**
     * The internal hasher representation.
     */
    private StaticHasher hasher;

    /**
     * Constructs a HasherBloomFilter from a hasher and a shape.
     * @param hasher the hasher to use.
     * @param shape the shape to use.
     */
    public HasherBloomFilter( Hasher hasher, Shape shape )
    {
        super( shape );
        verifyHasher(hasher);
        if (hasher instanceof StaticHasher)
        {
            this.hasher = (StaticHasher) hasher;
            verifyShape( this.hasher.getShape() );
        } else {
            this.hasher = new StaticHasher( hasher, shape );
        }
    }

    /**
     * Constructs an emtpy HasherBloomFilter from a shape.
     * @param shape the shape to use.
     */
    public HasherBloomFilter( Shape shape )
    {
        super( shape );
        this.hasher = new StaticHasher( EmptyIterator.emptyIterator(), shape );
    }

    @Override
    public LongBuffer getBits() {
        if (hasher.size() == 0)
        {
            return LongBuffer.allocate(0);
        }
        int n = (int) Math.ceil( hasher.getShape().getNumberOfBits() / Long.SIZE );
        LongBuffer result = LongBuffer.allocate( n );
        OfInt iter = hasher.getBits(hasher.getShape());
        while (iter.hasNext())
        {
            iter.forEachRemaining( (IntConsumer) idx -> {
                long buff = result.get( idx / Long.SIZE );
                int pwr = Math.floorMod(idx, Long.SIZE);
                long buffOffset = (long) Math.pow(2, pwr);
                buff |= buffOffset;
                result.put( idx / Long.SIZE, buff ) ;
            });
        }
        return result;
    }

    @Override
    public StaticHasher getHasher() {
        return hasher;
    }

    @Override
    public void merge(BloomFilter other) {
        merge( other.getShape(), other.getHasher());
    }

    @Override
    public void merge(Shape shape, Hasher hasher) {
        verifyShape( shape );
        IteratorChain<Integer> iter = new IteratorChain<Integer>( this.hasher.getBits(shape), hasher.getBits(shape));
        this.hasher = new StaticHasher( iter, this.hasher.getShape() );
    }

    @Override
    public int cardinality() {
        return hasher.size();
    }

    @Override
    public boolean contains(Shape shape, Hasher hasher) {
        verifyShape(shape);
        Set<Integer> set = new TreeSet<Integer>();
        hasher.getBits(shape).forEachRemaining( (IntConsumer) idx -> {
            set.add( idx );
        });
        OfInt iter = this.hasher.getBits(shape);
        while (iter.hasNext()) {
            int idx = iter.nextInt();
            set.remove( idx );
            if (set.isEmpty())
            {
                return true;
            }
        }
        return false;
    }

}
