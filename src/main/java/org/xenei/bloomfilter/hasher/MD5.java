package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 implements Hasher.Func  {

    MessageDigest md;
    long[] result = null;

    public MD5() throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("MD5");
    }

    @Override
    public String getName() {
        return "MD5-SC";
    }

    @Override
    public long applyAsLong(ByteBuffer buffer, Integer seed) {

        if (result == null || seed == 0 )
        {
            byte[] hash;
            synchronized (md) {
                int p = buffer.position();
                md.update( buffer );
                hash = md.digest();
                md.reset();
                buffer.position( p );
            }

            LongBuffer lb = ByteBuffer.wrap( hash ).asLongBuffer();
            result[0] = lb.get(0);
            result[1] = lb.get(1);
        } else {
            result[0] += result[1];
        }
        return result[0];
    }

}
