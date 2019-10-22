package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.junit.Test;
import org.xenei.bloomfilter.hasher.MurmurHash3.LongPair;

public class Murmur128Test {



    @Test
    public void test() throws Exception {
        Murmur128 murmur = new Murmur128();

        long l1 = 0xe7eb60dabb386407L;
        long l2 = 0xc3ca49f691f73056L;
        ByteBuffer buffer = ByteBuffer.wrap( "Now is the time for all good men to come to the aid of their country".getBytes("UTF-8") );

        long l = murmur.applyAsLong( buffer ,  0 );
        assertEquals( l1, l );
        l = murmur.applyAsLong( buffer,  1 );
        assertEquals( l1+l2, l );
        l = murmur.applyAsLong( buffer,  2 );
        assertEquals( l1+l2+l2, l );
    }

}
