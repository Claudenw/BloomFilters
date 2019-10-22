package org.xenei.bloomfilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xenei.bloomfilter.BloomFilter.Shape;
import org.xenei.bloomfilter.Hasher.Func;

public class HasherTest {

    @After
    public void teardown() {
        Hasher.resetFuncs();
    }

    @Test
    public void testListFuncs() {
        Set<String> names = Hasher.listFuncs();
        assertEquals( 4, names.size() );
        assertTrue( names.contains( "MD5-SC"));
        assertTrue( names.contains( "Murmur3_x64_128-SC"));
        assertTrue( names.contains( "Murmur3_x86_32-SI"));
        assertTrue( names.contains( "Objects32-SI"));
    }

    @Test
    public void testGetFunc_and_name() {
        Hasher h = Hasher.getHasher("MD5-SC");
        assertEquals( "MD5-SC", h.getName());
    }

    @Test
    public void testRegisterAndResetFunc() throws Exception {
        Hasher.register( "TestFunc", TestFunc.class );
        Set<String> names = Hasher.listFuncs();
        assertEquals( 5, names.size() );
        assertTrue( names.contains( "TestFunc" ));
        Hasher.resetFuncs();
        names = Hasher.listFuncs();
        assertEquals( 4, names.size() );
        assertFalse( names.contains( "TestFunc" ));
    }

    @Test
    public void testIter() throws Exception
    {
        Hasher.register( "TestFunc", TestFunc.class );
        Hasher hasher = Hasher.getHasher("TestFunc");
        hasher.with( "Hello");
        Shape shape = new Shape( hasher, 3, 72, 17);
        OfInt iter = hasher.getBits(shape);
        for (int i=0;i<17;i++)
        {
            assertTrue( iter.hasNext() );
            assertEquals( i, iter.nextInt() );
        }
        assertFalse( iter.hasNext() );

    }

    @Test
    public void testIter_MultipleHashes() throws Exception
    {
        Hasher.register( "TestFunc", TestFunc.class );
        Hasher hasher = Hasher.getHasher("TestFunc");
        hasher.with( "Hello").with("world");
        Shape shape = new Shape( hasher, 3, 72, 17);
        OfInt iter = hasher.getBits(shape);
        for (int j=0;j<2;j++)
        {
        for (int i=0;i<17;i++)
        {
            assertTrue( iter.hasNext() );
            assertEquals( i, iter.nextInt() );
        }
        }
        assertFalse( iter.hasNext() );

    }


    @Test
    public void testLockedAfterIter() throws Exception
    {
        Hasher.register( "TestFunc", TestFunc.class );
        Hasher hasher = Hasher.getHasher("TestFunc");
        hasher.with( "Hello");
        Shape shape = new Shape( hasher, 3, 72, 17);
        OfInt iter = hasher.getBits(shape);
        try {
            hasher.with( "World" );
            fail( "Should have thrown IllegalStateException");
        }
        catch (IllegalStateException expectd)
        {
            // do nothing.
        }

    }


    public static class TestFunc implements Func {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return seed;
        }

    }
}
