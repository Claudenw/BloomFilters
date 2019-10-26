package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.junit.Test;

public class ObjectsHashTest {

    @Test
    public void test() throws Exception {
        ObjectsHash obj = new ObjectsHash();

        ByteBuffer buffer = ByteBuffer
                .wrap("Now is the time for all good men to come to the aid of their country".getBytes("UTF-8"));


        long l = obj.applyAsLong(buffer, 0);
        long prev = 0;
        assertEquals(Objects.hash(prev, buffer), l);
        prev += l;
        l = obj.applyAsLong(buffer, 1);
        assertEquals(Objects.hash(prev, buffer), l);
        prev += l;
        l = obj.applyAsLong(buffer, 2);
        assertEquals(Objects.hash(prev, buffer), l);
    }

}
