package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.xenei.bloomfilter.Hasher;

public class ObjectsHash implements Hasher.Func  {

   public static final String name = "Objects32-SI";

    @Override
    public long applyAsLong(ByteBuffer buffer, Integer seed) {
        return Objects.hash( seed, buffer.duplicate().position(0) );
    }

}
