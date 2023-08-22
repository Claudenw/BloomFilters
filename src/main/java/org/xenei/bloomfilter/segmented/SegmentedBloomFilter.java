package org.xenei.bloomfilter.segmented;

import java.util.function.IntPredicate;
import java.util.function.LongPredicate;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

public class SegmentedBloomFilter implements BloomFilter {

    @Override
    public boolean forEachIndex(IntPredicate predicate) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean forEachBitMap(LongPredicate predicate) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public BloomFilter copy() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int characteristics() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Shape getShape() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean contains(IndexProducer indexProducer) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean merge(IndexProducer indexProducer) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean merge(BitMapProducer bitMapProducer) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int cardinality() {
        // TODO Auto-generated method stub
        return 0;
    }

}
