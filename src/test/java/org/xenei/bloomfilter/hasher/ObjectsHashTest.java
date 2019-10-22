package org.xenei.bloomfilter.hasher;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.junit.Test;
import org.xenei.bloomfilter.hasher.MurmurHash3.LongPair;

public class ObjectsHashTest {



    @Test
    public void test() throws Exception {
        ObjectsHash obj = new ObjectsHash();

        ByteBuffer buffer = ByteBuffer.wrap( "Now is the time for all good men to come to the aid of their country".getBytes("UTF-8") );


     long l = obj.applyAsLong( buffer ,  0 );
        assertEquals( Objects.hash( Integer.valueOf(0), buffer ), l );
        l = obj.applyAsLong( buffer,  1 );
        assertEquals( Objects.hash( Integer.valueOf(1), buffer ), l );
        l = obj.applyAsLong( buffer,  2 );
        assertEquals( Objects.hash( Integer.valueOf(2), buffer ), l );
    }

}
