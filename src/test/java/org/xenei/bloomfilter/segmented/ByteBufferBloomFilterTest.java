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
package org.xenei.bloomfilter.segmented;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.bloomfilter.AbstractBloomFilterTest;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public class ByteBufferBloomFilterTest extends AbstractBloomFilterTest<ByteBufferBloomFilter> {

    @Override
    protected ByteBufferBloomFilter createEmptyFilter(Shape shape) {
        return new ByteBufferBloomFilter(shape);
    }

    @Test
    public void shiftTest() {
        for (int i = 0; i < 8; i++) {
            int expected = 1 << i;
            assertEquals(expected, ByteBufferBloomFilter.getIntBit(i), "failed at " + i);
            assertEquals(0, ByteBufferBloomFilter.getByteIndex(i));
        }
        for (int i = 8; i < 16; i++) {
            int expected = 1 << (i - 8);
            assertEquals(expected, ByteBufferBloomFilter.getIntBit(i), "failed at " + i);
            assertEquals(1, ByteBufferBloomFilter.getByteIndex(i));
        }

        for (int i = 16; i < 24; i++) {
            int expected = 1 << (i - 16);
            assertEquals(expected, ByteBufferBloomFilter.getIntBit(i), "failed at " + i);
            assertEquals(2, ByteBufferBloomFilter.getByteIndex(i));
        }

        for (int i = 24; i < 32; i++) {
            int expected = 1 << (i - 24);
            assertEquals(expected, ByteBufferBloomFilter.getIntBit(i), "failed at " + i);
            assertEquals(3, ByteBufferBloomFilter.getByteIndex(i));
        }

        for (int i = 32; i < 40; i++) {
            int expected = 1 << (i - 32);
            assertEquals(expected, ByteBufferBloomFilter.getIntBit(i), "failed at " + i);
            assertEquals(4, ByteBufferBloomFilter.getByteIndex(i));
        }

    }

    @Test
    public void testBitmapWithWrap() {
        final Shape shape = Shape.fromKM(17, 72);
        final long[] values = { 0x706050403020100L, 0x8L };
        final BloomFilter f = createFilter(shape, BitMapProducer.fromBitMapArray(values));
        final List<Long> lst = new ArrayList<>();
        f.forEachBitMap(l -> lst.add(l));
        assertEquals(0x706050403020100L, lst.get(0));
        assertEquals(0x8, lst.get(1));
    }

    int getByte(long l, int pos) {
        int shift = pos * Byte.SIZE;
        long l2 = 0xFFl << shift;
        return (int) ((l & l2) >> shift);
    }

    boolean x(ByteBuffer bb, int bbIdx, long l, int lIdx) {
        // System.out.format( "bb: %x \tl: %x\n", bb.get(bbIdx), getByte( l, lIdx ));
        return (bb.get(bbIdx) & 0xFF) == getByte(l, lIdx);
    }

    @Test
    public void testMergeWithBitMapProducer2() {
        final Shape shape = Shape.fromKM(17, 72);
        final long[] values = { 2310702850776858929l, 6l };
        ByteBuffer bb = ByteBuffer.allocate(ByteBufferBloomFilter.numberOfBytes(shape.getNumberOfBits()));
        final BloomFilter f = new ByteBufferBloomFilter(shape, bb);
        f.merge(BitMapProducer.fromBitMapArray(values));
        boolean result = true;
        result &= x(bb, 0, values[0], 0);
        result &= x(bb, 1, values[0], 1);
        result &= x(bb, 2, values[0], 2);
        result &= x(bb, 3, values[0], 3);
        result &= x(bb, 4, values[0], 4);
        result &= x(bb, 5, values[0], 5);
        result &= x(bb, 6, values[0], 6);
        result &= x(bb, 7, values[0], 7);
        result &= x(bb, 8, values[1], 0);
        assertTrue(result);
        final List<Long> lst = new ArrayList<>();
        for (final long l : values) {
            lst.add(l);
        }
        assertTrue(f.forEachBitMap(l -> lst.remove(Long.valueOf(l))));
        assertTrue(lst.isEmpty());
    }

}
