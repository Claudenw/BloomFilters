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

import org.apache.commons.collections4.bloomfilter.AbstractIndexProducerTest;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IncrementingHasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

public class IndexProducerFromStableBloomFilterTest extends AbstractIndexProducerTest {

    StableShape stableShape = StableShape.builder(Shape.fromKM(17, 72)).build();

    @Override
    protected IndexProducer createProducer() {
        final Hasher hasher = new IncrementingHasher(0, 1);
        final BloomFilter bf = new StableBloomFilter(stableShape);
        bf.merge(hasher);
        return bf;
    }

    @Override
    protected IndexProducer createEmptyProducer() {
        return new StableBloomFilter(stableShape);
    }

    @Override
    protected int getAsIndexArrayBehaviour() {
        // Bit maps will be distinct. Conversion to indices should be ordered.
        return DISTINCT | ORDERED;
    }

    @Override
    protected int[] getExpectedIndices() {
        return new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    }
}
