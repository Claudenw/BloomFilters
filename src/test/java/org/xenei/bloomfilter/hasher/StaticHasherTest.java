package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.ToLongBiFunction;
import org.junit.After;
import org.junit.Test;
import org.xenei.bloomfilter.HasherFactory;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.hasher.DynamicHasher;

public class StaticHasherTest {

    @After
    public void teardown() {
        HasherFactory.resetFuncs();
    }

    @Test
    public void testIter() throws Exception {
        HasherFactory.register("TestFunc", TestFunc.class);
        DynamicHasher dHasher = HasherFactory.getHasher("TestFunc");
        dHasher.with("Hello");
        Shape shape = new Shape(dHasher, 3, 72, 17);
        StaticHasher hasher = new StaticHasher( dHasher, shape );
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
        DynamicHasher dHasher = HasherFactory.getHasher("TestFunc");
        dHasher.with("Hello");
        Shape shape = new Shape(dHasher, 3, 72, 17);
        StaticHasher hasher = new StaticHasher( dHasher, shape );
        Set<Integer> expected = new HashSet<Integer>();
        dHasher.getBits(shape).forEachRemaining((Consumer<Integer>) expected::add );

        OfInt iter = hasher.getBits(shape);
        while (iter.hasNext())
        {
            Integer i = iter.next();
            assertTrue( "Value missing: "+i, expected.remove( i ));
        }
        assertTrue( "did not find all the values", expected.isEmpty());
    }

    public static class TestFunc implements ToLongBiFunction<ByteBuffer, Integer> {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return seed;
        }

    }
}
