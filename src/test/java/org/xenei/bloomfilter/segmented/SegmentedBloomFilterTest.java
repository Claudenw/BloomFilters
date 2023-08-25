package org.xenei.bloomfilter.segmented;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.TestingHashers;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;

import org.junit.jupiter.api.Test;

public class SegmentedBloomFilterTest {

    private Shape keyShape = Shape.fromNP(7, 0.1);

    enum Keys {
        Name, Address, DOB, Phone
    }

    @Test
    public void testConstruction() {
        assertEquals(34, keyShape.getNumberOfBits());
        assertEquals(3, keyShape.getNumberOfHashFunctions());
        SegmentedBloomFilter filter = new SegmentedBloomFilter(keyShape, Keys.values().length);
        // keyShape uses 5 bytes
        // 4 keys * 5bytes * 8bits_per_byte = 160
        assertEquals(160, filter.getShape().getNumberOfBits());
        assertEquals(3 * 4, filter.getShape().getNumberOfHashFunctions());
        BloomFilter bf = filter.getSegment(0);
        assertEquals(keyShape, bf.getShape());
        assertEquals(4, filter.getSegmentCount());
    }

    @Test
    public void testSegmentUpdate() {
        SegmentedBloomFilter filter = new SegmentedBloomFilter(keyShape, Keys.values().length);

        filter.getSegment(0).merge(TestingHashers.FROM1);
        assertTrue(filter.forEachBitMapPair(BitMapProducer.fromBitMapArray(0xE, 0, 0), (a, b) -> a == b));

        filter.getSegment(1).merge(TestingHashers.FROM1);
        assertTrue(filter.forEachBitMapPair(BitMapProducer.fromBitMapArray(0xE000000000EL, 0, 0), (a, b) -> a == b));

        filter.getSegment(2).merge(TestingHashers.FROM1);
        assertTrue(
                filter.forEachBitMapPair(BitMapProducer.fromBitMapArray(0xE000000000EL, 0xE0000, 0), (a, b) -> a == b));

        filter.getSegment(3).merge(TestingHashers.FROM1);
        assertTrue(filter.forEachBitMapPair(BitMapProducer.fromBitMapArray(0xE000000000EL, 0xE000000000E0000L, 0),
                (a, b) -> a == b));
    }

    private SegmentedBloomFilter create(Shape keyShape, String name) {
        return create(keyShape, name, name + "'s home", name + "'s DOB", name + "'s phone");
    }

    private SegmentedBloomFilter create(Shape keyShape, String name, String address, String dob, String phone) {

        SegmentedBloomFilter filter = new SegmentedBloomFilter(keyShape, Keys.values().length);
        if (name != null) {
        filter.getSegment(Keys.Name.ordinal()).merge(getHasher(name));
        }
        if (address != null) {
        filter.getSegment(Keys.Address.ordinal()).merge(getHasher(address));
        }
        if (dob != null) {
        filter.getSegment(Keys.DOB.ordinal()).merge(getHasher(dob));
        }
        if (phone != null) {
        filter.getSegment(Keys.Phone.ordinal()).merge(getHasher(phone));
        }
        return filter;
    }

    @Test
    public void testSegmentMatch() {
        SegmentedBloomFilter filter = new SegmentedBloomFilter(keyShape, Keys.values().length);

        BloomFilter joe = create(keyShape, "joe");
        BloomFilter marty = create(keyShape, "marty");
        BloomFilter mary = create(keyShape, "mary");
        BloomFilter bob = create(keyShape, "bob");

        filter.merge(joe);
        filter.merge(marty);
        filter.merge(mary);
        filter.merge(bob);

        assertTrue(filter.contains(joe));
        assertTrue(filter.contains(marty));
        assertTrue(filter.contains(mary));
        assertTrue(filter.contains(bob));

        BloomFilter falsePositive = create(keyShape, "joe", "marty's home", "joe's DOB", "joe's phone");
        assertTrue(filter.contains(falsePositive));

        BloomFilter negative = create(keyShape, "joe", "jacks's home", "joe's DOB", "joe's phone");
        assertFalse(filter.contains(negative));

        
        BloomFilter target = create(keyShape, "joe", null, "joe's DOB", "joe's phone");
        assertTrue(filter.contains(target));
    }

    @Test
    public void testCrossImplementationSegmentMatch() {
        SegmentedBloomFilter filter = new SegmentedBloomFilter(keyShape, Keys.values().length);

        BloomFilter joe = create(keyShape, "joe");
        BloomFilter marty = create(keyShape, "marty");
        BloomFilter mary = create(keyShape, "mary");
        BloomFilter bob = create(keyShape, "bob");

        filter.merge(joe);
        filter.merge(marty);
        filter.merge(mary);
        filter.merge(bob);
        
        BloomFilter standard = new SimpleBloomFilter(joe.getShape());
        standard.merge(filter);

        assertTrue(standard.contains(joe));
        assertTrue(standard.contains(marty));
        assertTrue(standard.contains(mary));
        assertTrue(standard.contains(bob));

        BloomFilter falsePositive = create(keyShape, "joe", "marty's home", "joe's DOB", "joe's phone");
        assertTrue(standard.contains(falsePositive));
        
        BloomFilter negative = create(keyShape, "joe", "jacks's home", "joe's DOB", "joe's phone");
        assertFalse(filter.contains(negative));

        BloomFilter target = create(keyShape, "joe", null, "joe's DOB", "joe's phone");
        assertTrue(standard.contains(target));

        standard.clear();
        standard.merge(joe);
        standard.merge(marty);
        standard.merge(mary);
        standard.merge(bob);
        
        assertTrue(standard.contains(joe));
        assertTrue(standard.contains(marty));
        assertTrue(standard.contains(mary));
        assertTrue(standard.contains(bob));

        falsePositive = create(keyShape, "joe", "marty's home", "joe's DOB", "joe's phone");
        assertTrue(standard.contains(falsePositive));

        negative = create(keyShape, "joe", "jacks's home", "joe's DOB", "joe's phone");
        assertFalse(filter.contains(negative));

        target = create(keyShape, "joe", null, "joe's DOB", "joe's phone");
        assertTrue(standard.contains(target));
    }


    static Hasher getHasher(String value) {
        long[] parts = MurmurHash3.hash128x64(value.getBytes(StandardCharsets.UTF_8));
        return new EnhancedDoubleHasher(parts[0], parts[1]);
    }

}
