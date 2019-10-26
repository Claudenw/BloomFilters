package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.ToLongBiFunction;

public class MD5 implements ToLongBiFunction<ByteBuffer, Integer> {

    private MessageDigest md;
    private long[] result = null;
    public static final String name = "MD5-SC";

    public MD5() throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("MD5");
    }

    @Override
    public long applyAsLong(ByteBuffer buffer, Integer seed) {

        if (result == null || seed == 0) {
            result = new long[2];
            byte[] hash;
            synchronized (md) {
                md.update(buffer.duplicate().position(0));
                hash = md.digest();
                md.reset();
            }

            LongBuffer lb = ByteBuffer.wrap(hash).asLongBuffer();
            lb.position(0);
            System.out.println(lb.limit());
            result[0] = lb.get(0);
            result[1] = lb.get(1);
        } else {
            result[0] += result[1];
        }
        return result[0];
    }

}
