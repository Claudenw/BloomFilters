package org.xenei.bloomfilter.segmented;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

public class SegmentedBloomFilter implements BloomFilter { 
    
    private final ByteBufferBloomFilter master;
    private final ByteBufferBloomFilter[] segments;
    private final ByteBuffer buffer;
    
    private static int bufferLength(Shape keyShape, int keyCount) {
        return ByteBufferBloomFilter.numberOfBytes(keyShape.getNumberOfBits()) * keyCount;
    }
    
    private static Shape masterShape(Shape keyShape, int keyCount) {
        return Shape.fromKM(keyShape.getNumberOfHashFunctions()*keyCount, bufferLength(keyShape, keyCount) * Byte.SIZE);
    }

    public SegmentedBloomFilter(Shape keyShape, int keyCount) {
        this(ByteBuffer.allocate(bufferLength(keyShape, keyCount)), masterShape(keyShape, keyCount), keyShape, keyCount);
    }

    private SegmentedBloomFilter(ByteBuffer buffer, Shape masterShape, Shape keyShape, int keyCount) {
        this.buffer = buffer;
        this.master = new ByteBufferBloomFilter(masterShape, buffer);
        this.segments = new ByteBufferBloomFilter[keyCount];
        int numberOfKeyBytes = ByteBufferBloomFilter.numberOfBytes(keyShape.getNumberOfBits());
        int offset = 0;
        for (int i=0;i<keyCount; i++) {
            buffer.limit(offset+numberOfKeyBytes);
            buffer.position(offset);
            this.segments[i] = new ByteBufferBloomFilter(keyShape, buffer, offset);
            offset += numberOfKeyBytes;
        }
        buffer.position(0);
    }

    @Override
    public boolean forEachIndex(IntPredicate predicate) {
        return master.forEachIndex(predicate);
    }

    @Override
    public boolean forEachBitMap(LongPredicate predicate) {
        return master.forEachBitMap(predicate);
    }

    @Override
    public BloomFilter copy() {
        ByteBuffer buffer = ByteBuffer.allocate(this.buffer.capacity());
        return new SegmentedBloomFilter(ByteBuffer.allocate(this.buffer.capacity()), master.getShape(), segments[0].getShape(), segments.length);
    }

    @Override
    public int characteristics() {
        return 0;
    }

    @Override
    public Shape getShape() {
        return master.getShape();
    }

    @Override
    public void clear() {
        master.clear();
    }

    @Override
    public boolean contains(IndexProducer indexProducer) {
        return master.contains(indexProducer);
    }

    @Override
    public boolean merge(IndexProducer indexProducer) {
        return master.merge(indexProducer);
    }

    @Override
    public boolean merge(BitMapProducer bitMapProducer) {
        return master.merge(bitMapProducer);
    }

    @Override
    public int cardinality() {
        return Arrays.stream(segments).mapToInt(BloomFilter::cardinality).sum();
    }
    
    public BloomFilter getSegment(int idx) {
        return segments[idx];
    }
    
    public int getSegmentCount() {
        return segments.length;
    }
}
