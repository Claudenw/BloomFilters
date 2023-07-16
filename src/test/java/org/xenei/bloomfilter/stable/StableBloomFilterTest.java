package org.xenei.bloomfilter.stable;

import org.apache.commons.collections4.bloomfilter.AbstractBloomFilterTest;
import org.apache.commons.collections4.bloomfilter.Shape;

/**
 * Tests for the {@link StableBloomFilter}.
 */
public class StableBloomFilterTest extends AbstractBloomFilterTest<StableBloomFilter> {

    @Override
    protected StableBloomFilter createEmptyFilter(final Shape shape) {
        StableShape stableShape = StableShape.builder(shape).setP(0).build();
        return new StableBloomFilter(stableShape);
    }
}
