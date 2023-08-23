package org.xenei.bloomfilter.segmented;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.TestingHashers;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.LongBiPredicate;
import org.junit.jupiter.api.Test;

public class SegmentedBloomFilterTest {
    
    private Shape keyShape = Shape.fromNP(7, 0.1);
    
    enum Keys { Name, Address, DOB, Phone };

    @Test
    public void testConstruction() {
        assertEquals( 34, keyShape.getNumberOfBits());
        assertEquals( 3, keyShape.getNumberOfHashFunctions());
        SegmentedBloomFilter filter = new SegmentedBloomFilter( keyShape, Keys.values().length );
        // keyShape uses 5 bytes 
        // 4 keys * 5bytes * 8bits_per_byte = 160
        assertEquals( 160, filter.getShape().getNumberOfBits() );
        assertEquals( 3*4, filter.getShape().getNumberOfHashFunctions());
        BloomFilter bf = filter.getSegment(0);
        assertEquals(keyShape, bf.getShape());
        assertEquals(4, filter.getSegmentCount());
    }
    
    @Test
    public void testSegmentUpdate() {
        SegmentedBloomFilter filter = new SegmentedBloomFilter( keyShape, Keys.values().length );
        
        filter.getSegment(0).merge(TestingHashers.FROM1);
        assertTrue(filter.forEachBitMapPair( BitMapProducer.fromBitMapArray( 0xE, 0, 0 ), (a,b) -> a == b ));
        
        filter.getSegment(1).merge(TestingHashers.FROM1);
        assertTrue(filter.forEachBitMapPair( BitMapProducer.fromBitMapArray( 0xE000000000EL, 0, 0 ), (a,b) -> a == b ));
        
        filter.getSegment(2).merge(TestingHashers.FROM1);
        assertTrue(filter.forEachBitMapPair( BitMapProducer.fromBitMapArray( 0xE000000000EL, 0xE0000, 0 ), (a,b) -> a == b ));
        
        filter.getSegment(3).merge(TestingHashers.FROM1);
        assertTrue(filter.forEachBitMapPair( BitMapProducer.fromBitMapArray( 0xE000000000EL, 0xE000000000E0000L, 0 ), (a,b) -> a == b ));
    }
    
    public void testSegmentMatch() {
        fail("not implemented");
    }

}
