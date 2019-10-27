package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.ToLongBiFunction;
import org.junit.After;
import org.junit.Test;
import org.xenei.bloomfilter.HasherFactory;
import org.xenei.bloomfilter.HasherFactory.Hasher;
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
        assertEquals( 17, hasher.size());
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

        assertEquals( expected.size(), hasher.size() );

        OfInt iter = hasher.getBits(shape);
        while (iter.hasNext())
        {
            Integer i = iter.next();
            assertTrue( "Value missing: "+i, expected.remove( i ));
        }
        assertTrue( "did not find all the values", expected.isEmpty());
    }

    @Test
    public void testConstructor_Iterator() throws Exception {
        Hasher h = new Hasher() {

            @Override
            public String getName() {
                return "TestHasher";
            }

            @Override
            public OfInt getBits(Shape shape) {
                return null;
            }};

        Shape shape = new Shape(h, 3, 72, 17);
        int[] values = { 1, 3, 5, 7, 9, 3, 5, 1};
        Iterator<Integer> iter = Arrays.stream(values).iterator();
        StaticHasher hasher = new StaticHasher(iter, shape );

        assertEquals( 5, hasher.size() );
        assertEquals( shape, hasher.getShape());
        assertEquals( "TestHasher", hasher.getName());

        iter = hasher.getBits(shape);
        int idx = 0;
        while (iter.hasNext())
        {
            assertEquals( "Error at idx "+idx, Integer.valueOf(values[idx]), iter.next());
            idx++;
        }
        assertEquals( 5, idx );
    }

    public static class TestFunc implements ToLongBiFunction<ByteBuffer, Integer> {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return seed;
        }

    }
}
