package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

public class MD5Test {

    @Test
    public void test() throws Exception {
        MD5 md5 = new MD5();
        long l1 = 0x8b1a9953c4611296L;
        long l2 = 0xa827abf8c47804d7L;
        ByteBuffer buffer = ByteBuffer.wrap("Hello".getBytes());

        long l = md5.applyAsLong(buffer, 0);
        assertEquals(l1, l);
        l = md5.applyAsLong(buffer, 1);
        assertEquals(l1 + l2, l);
        l = md5.applyAsLong(buffer, 2);
        assertEquals(l1 + l2 + l2, l);
    }

}
