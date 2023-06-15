package org.xenei.bloomfilter.layered;

import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.AbstractBloomFilterTest;
import org.apache.commons.collections4.bloomfilter.Shape;

public class SimpleLayeredFilterTest extends AbstractLayeredFilterTest<SimpleLayeredFilter> {

    @Override
    protected SimpleLayeredFilter createEmptyFilter(Shape shape) {
        return new SimpleLayeredFilter( shape, 10, LayeredBloomFilter.ADVANCE_ON_POPULATED );
    }

    @Override
    SimpleLayeredFilter createLayeredFilter(Shape shape, int maxDepth, Predicate<LayeredBloomFilter> extendCheck) {
        return new SimpleLayeredFilter( shape, maxDepth, extendCheck );
    }

}
