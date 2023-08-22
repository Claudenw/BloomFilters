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

import java.nio.ByteBuffer;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

public class ByteBufferBloomFilter implements BloomFilter {
    private ByteBuffer buffer;
    private Shape shape;
    private int cardinality = -1;

    private static int[] cardTable = new int[0xFF + 1];

    static {
        for (int i = 1; i <= 0xFF; i++) {
            cardTable[i] = Integer.bitCount(i);
        }
    }

    /**
     * bit shift 3 = divide by 8 // multiply by 8
     */
    private final static int BY_8 = 3;

    public ByteBufferBloomFilter(Shape shape) {
        this(shape, null, 0);
    }

    public ByteBufferBloomFilter(Shape shape, ByteBuffer buffer) {
        this(shape, buffer, buffer.position());
    }

    public ByteBufferBloomFilter(Shape shape, ByteBuffer buffer, int position) {
        this.buffer = (buffer == null) ? ByteBuffer.allocate(numberOfBytes(shape.getNumberOfBits()))
                : (position == 0) ? buffer.duplicate() : buffer.slice();
        this.shape = shape;
        int limit = numberOfBytes(shape.getNumberOfBits());
        if (limit > this.buffer.capacity()) {
            throw new IllegalArgumentException("Buffer capacity is too small");
        }
        if (limit < this.buffer.capacity()) {
            this.buffer.limit(limit);
        }
    }

    /**
     * Gets the filter index for the specified bit index assuming the filter is
     * using 8-bit bytes to store bits starting at index 0.
     *
     * <p>The index is assumed to be positive. For a positive index the result will
     * match {@code bitIndex / 8}.</p>
     *
     * <p><em>The divide is performed using bit shifts. If the input is negative the
     * behavior is not defined.</em></p>
     *
     * @param  bitIndex the bit index (assumed to be positive)
     * @return          the index of the bit map in an array of bit maps.
     */
    public static int getByteIndex(final int bitIndex) {
        // An integer divide by 8 is equivalent to a shift of 3 bits if the integer is
        // positive.
        // We do not explicitly check for a negative here. Instead we use a
        // signed shift. Any negative index will produce a negative value
        // by sign-extension and if used as an index into an array it will throw an
        // exception.
        return bitIndex >> BY_8;
    }

    /**
     * Calculates the number of bytes required for the numberOfBits parameter.
     *
     * <p><em>If the input is negative the behavior is not defined.</em></p>
     *
     * @param  numberOfBits the number of bits to store in the array of bit maps.
     * @return              the number of bit maps necessary.
     */
    public static int numberOfBytes(final int numberOfBits) {
        return (numberOfBits - 1 >> BY_8) + 1;
    }

    /**
     * Gets the filter bit mask for the specified bit index assuming the filter is
     * using 64-bit longs to store bits starting at index 0. The returned value is a
     * {@code long} with only 1 bit set.
     *
     * <p>The index is assumed to be positive. For a positive index the result will
     * match {@code 1L << (bitIndex % 64)}.</p>
     *
     * <p><em>If the input is negative the behavior is not defined.</em></p>
     *
     * @param  bitIndex the bit index (assumed to be positive)
     * @return          the filter bit
     */
    public static int getIntBit(final int bitIndex) {
        // Bit shifts only use the first 6 bits. Thus it is not necessary to mask this
        // using 0x3f (63) or compute bitIndex % 64.
        // Note: If the index is negative the shift will be (64 - (bitIndex & 0x3f)) and
        // this will identify an incorrect bit.
        int result = 1 << bitIndex;
        int mask;
        for (int shift = 0; shift < 32; shift += 8) {
            mask = 0xFF << shift;
            if ((result & mask) != 0) {
                return (mask & result) >> shift & 0xFF;
            }
        }
        throw new IllegalArgumentException("Index ouf of range: " + bitIndex);
    }

    private boolean forEachIndex(byte bb, int offset, IntPredicate predicate) {
        int b = bb & 0xFF;
        int i = offset;
        while (b != 0) {
            if ((b & 1) == 1 && !predicate.test(i)) {
                return false;
            }
            b >>>= 1;
            i++;
        }
        return true;
    }

    @Override
    public boolean forEachIndex(IntPredicate predicate) {
        int limit = numberOfBytes(shape.getNumberOfBits());
        for (int idx = 0; idx < limit; idx++) {
            if (!forEachIndex(buffer.get(idx), idx * 8, predicate)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachBitMap(LongPredicate predicate) {
        long result = 0;
        int shift = 0;
        for (int i = 0; i < buffer.limit(); i++) {
            long b = buffer.get(i) & 0xFF;
            long x = b << shift;
            result |= (b << shift);
            shift += Byte.SIZE;
            if ((shift % Long.SIZE) == 0) {
                if (!predicate.test(result)) {
                    return false;
                }
                result = 0;
            }
        }
        return ((shift % Long.SIZE) != 0) ? predicate.test(result) : true;
    }

    @Override
    public BloomFilter copy() {
        ByteBufferBloomFilter result = new ByteBufferBloomFilter(shape);
        int numberOfBytes = numberOfBytes(shape.getNumberOfBits());
        if (this.buffer.hasArray()) {
            result.buffer.put(this.buffer.array(), this.buffer.arrayOffset(), numberOfBytes);
        } else {
            byte[] xfer = new byte[1024 * 1024];
            int offset = 0;
            int limit;
            while (offset < result.buffer.limit()) {
                limit = (offset + xfer.length > result.buffer.limit()) ? result.buffer.limit() : offset + xfer.length;
                this.buffer.get(xfer, offset, limit);
                result.buffer.put(xfer, offset, limit);
                offset += limit;
            }
        }
        return result;
    }

    @Override
    public int characteristics() {
        return BloomFilter.SPARSE;
    }

    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public void clear() {
        for (int idx = 0; idx < buffer.limit(); idx++) {
            buffer.put(idx, (byte) 0);
        }
        cardinality = -1;
    }

    @Override
    public boolean contains(IndexProducer indexProducer) {
        return indexProducer.forEachIndex(i -> (buffer.get(getByteIndex(i)) & getIntBit(i)) != 0);
    }

    @Override
    public boolean merge(IndexProducer indexProducer) {
        cardinality = -1;
        return indexProducer.forEachIndex(i -> {
            if (i > -1 && i < shape.getNumberOfBits()) {
                int idx = getByteIndex(i);
                buffer.put(idx, (byte) (buffer.get(idx) | getIntBit(i)));
                return true;
            } else {
                throw new IllegalArgumentException(String
                        .format("Bloom filter only accepts indexes in the range [0..%s)", shape.getNumberOfBits()));
            }
        });
    }

    @Override
    public boolean merge(BitMapProducer bitMapProducer) {
        // return merge(IndexProducer.fromBitMapProducer(bitMapProducer));
        int idx[] = new int[] { 0 };
        cardinality = -1;
        return bitMapProducer.forEachBitMap(l -> {
            int bufferIdx;
            int shift;
            for (int i = 0; i < Long.BYTES; i++) {
                shift = i * Byte.SIZE;
                long mask = 0xFFL << shift;
                if ((l & mask) != 0) {
                    bufferIdx = idx[0] + i;
                    if (bufferIdx >= buffer.limit()) {
                        throw new IllegalArgumentException(
                                String.format("BitMapProducer set a bit higher than the limit for the shape: %s",
                                        shape.getNumberOfBits()));
                    }
                    long b = (buffer.get(bufferIdx) & 0xFF) | (((l & mask) >> shift) & 0xFF);
                    buffer.put(bufferIdx, (byte) b);
                }
            }
            idx[0] += Long.BYTES;
            return true;
        });
    }

    @Override
    public int cardinality() {
        int c = cardinality;
        if (c == -1) {
            c = 0;
            for (int idx = 0; idx < buffer.limit(); idx++) {
                c += cardTable[0xFF & buffer.get(idx)];
            }
            cardinality = c;
        }
        return c;
    }

}
