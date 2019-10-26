package org.xenei.bloomfilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.util.function.ToLongBiFunction;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.xenei.bloomfilter.HasherFactory.Hasher;

public class HasherFactoryTest {

    @After
    public void teardown() {
        HasherFactory.resetFuncs();
    }

    @Test
    public void testListFuncs() {
        Set<String> names = HasherFactory.listFuncs();
        assertEquals(4, names.size());
        assertTrue(names.contains("MD5-SC"));
        assertTrue(names.contains("Murmur3_x64_128-SC"));
        assertTrue(names.contains("Murmur3_x86_32-SI"));
        assertTrue(names.contains("Objects32-SI"));
    }

    @Test
    public void testGetFunc_and_name() {
        Hasher h = HasherFactory.getHasher("MD5-SC");
        assertEquals("MD5-SC", h.getName());
    }

    @Test
    public void testRegisterAndResetFunc() throws Exception {
        HasherFactory.register("TestFunc", TestFunc.class);
        Set<String> names = HasherFactory.listFuncs();
        assertEquals(5, names.size());
        assertTrue(names.contains("TestFunc"));
        HasherFactory.resetFuncs();
        names = HasherFactory.listFuncs();
        assertEquals(4, names.size());
        assertFalse(names.contains("TestFunc"));
    }

    public static class TestFunc implements ToLongBiFunction<ByteBuffer, Integer> {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return seed;
        }

    }
}
