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
package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.ToLongBiFunction;
import org.junit.After;
import org.junit.Test;
import org.xenei.bloomfilter.HasherFactory;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.hasher.DynamicHasher;

public class DynamicHasherTest {

    @After
    public void teardown() {
        HasherFactory.reset();
    }

    @Test
    public void testIter() throws Exception {
        HasherFactory.register("TestFunc", TestFunc.class);
        DynamicHasher hasher = HasherFactory.getHasher("TestFunc");
        hasher.with("Hello");
        Shape shape = new Shape(hasher, 3, 72, 17);
        OfInt iter = hasher.getBits(shape);
        for (int i = 0; i < 17; i++) {
            assertTrue(iter.hasNext());
            assertEquals(i, iter.nextInt());
        }
        assertFalse(iter.hasNext());

    }

    @Test
    public void testIter_MultipleHashes() throws Exception {
        HasherFactory.register("TestFunc", TestFunc.class);
        DynamicHasher hasher = HasherFactory.getHasher("TestFunc");
        hasher.with("Hello").with("world");
        Shape shape = new Shape(hasher, 3, 72, 17);
        OfInt iter = hasher.getBits(shape);
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 17; i++) {
                assertTrue(iter.hasNext());
                assertEquals(i, iter.nextInt());
            }
        }
        assertFalse(iter.hasNext());

    }

    @Test
    public void testLockedAfterIter() throws Exception {
        HasherFactory.register("TestFunc", TestFunc.class);
        DynamicHasher hasher = HasherFactory.getHasher("TestFunc");
        hasher.with("Hello");
        Shape shape = new Shape(hasher, 3, 72, 17);
        // cause the hasher to lock
        hasher.getBits(shape);
        try {
            hasher.with("World");
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException expectd) {
            // do nothing.
        }

    }

    public static class TestFunc implements ToLongBiFunction<ByteBuffer, Integer> {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return seed;
        }

    }
}
