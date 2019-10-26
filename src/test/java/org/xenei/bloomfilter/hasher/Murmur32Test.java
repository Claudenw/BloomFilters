package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.junit.Test;

public class Murmur32Test {

    @Test
    public void test() throws Exception {
        Murmur32 murmur = new Murmur32();

        ByteBuffer buffer = ByteBuffer
                .wrap("Now is the time for all good men to come to the aid of their country".getBytes("UTF-8"));

        long l = murmur.applyAsLong(buffer, 0);
        assertEquals(82674681, l);
        l = murmur.applyAsLong(buffer, 1);
        assertEquals(-1475490736, l);
        l = murmur.applyAsLong(buffer, 2);
        assertEquals(-1561435247, l);
    }

}
