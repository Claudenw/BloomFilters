package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.ToLongBiFunction;

public class ObjectsHash implements ToLongBiFunction<ByteBuffer, Integer> {

    public static final String name = "Objects32-SI";
    private long last = 0;

    @Override
    public long applyAsLong(ByteBuffer buffer, Integer seed) {
        if (seed == 0) {
            last = 0;
        }
        long result = Objects.hash(last, buffer.duplicate().position(0));
        last += result;
        return result;
    }

}
