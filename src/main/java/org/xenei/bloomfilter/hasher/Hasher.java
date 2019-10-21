package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntBiFunction;
import java.util.function.ToLongBiFunction;

import org.xenei.bloomfilter.BloomFilter.Shape;

public class Hasher {

    private static final Map<String,Func> funcMap;

    static {
        funcMap = new HashMap<String,Func>();
        try {
            register( new MD5() );
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException( "MD5 hash not found");
        }
        register( new Murmur128() );
        register( new ObjectsHash() );
    }

    public static void register( Func func ) {
        funcMap.put(func.getName(), func );
    }

    public Set<String> listFuncs() {
        return Collections.unmodifiableSet(funcMap.keySet());
    }

    public static Hasher getHasher( String funcName )
    {
        Func func = funcMap.get( funcName );
        if (func == null)
        {
            throw new IllegalArgumentException( "No Func implementation named "+funcName );
        }
        return new Hasher( func );
    }

    interface Func extends ToLongBiFunction<ByteBuffer,Integer> {
        String getName();
    }

    private final List<ByteBuffer> buffers;

    private final Func func;

    private Hasher( Func func )
    {
        this.buffers = new ArrayList<ByteBuffer>();
        this.func = func;
    }

    public String getName() {
        return func.getName();
    }

    public PrimitiveIterator.OfInt getBits(Shape shape) {
        if (!func.getName().equals(shape.getHasherName()))
        {
            throw new IllegalArgumentException( String.format("Shape hasher %s is not %s", shape.getHasherName(), func.getName()));
        }
        return new Iter(shape);
    }

    public final void with( ByteBuffer property )
    {
        buffers.add( property );
    }

    public final void with( byte property ) {
        with(ByteBuffer.wrap(new byte[] {property}));
    }

    public final void with( byte[] property ) {
        with( ByteBuffer.wrap( property ));
    }

    public final void with( String property ) {
        with(property.getBytes(StandardCharsets.UTF_8));
    }

    private class Iter implements PrimitiveIterator.OfInt {
        private int buffer = 0;
        private int funcCount = 0;
        private final Shape shape;

        private Iter(Shape shape) {
            this.shape = shape;
        }

        @Override
        public boolean hasNext() {
            return buffer < buffers.size() || funcCount < shape.getNumberOfHashFunctions();
        }

        @Override
        public int nextInt() {
            if (funcCount >= shape.getNumberOfHashFunctions())
            {
                funcCount = 0;
                buffer++;
            }
            return Math.floorMod( func.applyAsLong( buffers.get(buffer), funcCount++ ), shape.getNumberOfBits());
        }
    }

}
