package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.util.Objects;

public class ObjectsHash implements Hasher.Func  {

    @Override
    public String getName() {
        return "Objects32-SI";
    }

    @Override
    public long applyAsLong(ByteBuffer buffer, Integer seed) {
        return Objects.hash( buffer, seed );
    }

}
