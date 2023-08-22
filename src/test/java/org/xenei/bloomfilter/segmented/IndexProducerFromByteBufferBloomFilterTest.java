package org.xenei.bloomfilter.segmented;

import org.apache.commons.collections4.bloomfilter.AbstractIndexProducerTest;
import org.apache.commons.collections4.bloomfilter.IncrementingHasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

public class IndexProducerFromByteBufferBloomFilterTest extends AbstractIndexProducerTest {

    protected Shape shape = Shape.fromKM(17, 72);
    
    @Override
    protected IndexProducer createProducer() {
        ByteBufferBloomFilter filter = new ByteBufferBloomFilter(shape);
        filter.merge(new IncrementingHasher(0, 1));
        filter.merge(new IncrementingHasher(5, 1));
        return filter;
    }

    @Override
    protected IndexProducer createEmptyProducer() {
        return new ByteBufferBloomFilter(shape);
    }

    @Override
    protected int getAsIndexArrayBehaviour() {
        return DISTINCT | ORDERED;
    }

    @Override
    protected int[] getExpectedIndices() {
        return new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};
    }

}
