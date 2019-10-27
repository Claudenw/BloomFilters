package org.xenei.bloomfilter;

import java.nio.LongBuffer;
import java.util.Set;
import java.util.TreeSet;
import java.util.PrimitiveIterator.OfInt;
import java.util.function.IntConsumer;

import org.apache.commons.collections4.iterators.EmptyIterator;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.xenei.bloomfilter.HasherFactory.Hasher;
import org.xenei.bloomfilter.hasher.StaticHasher;

/**
 * A Bloom filter built on a single hasher.
 * This filter type should only be used for small filters (few on bits)
 *
 */
public class HasherBloomFilter extends BloomFilter {

    private StaticHasher hasher;

    public HasherBloomFilter( Hasher hasher, Shape shape )
    {
        super( shape );
        if (hasher instanceof StaticHasher)
        {
            this.hasher = (StaticHasher) hasher;
        } else {
            this.hasher = new StaticHasher( hasher, shape );
        }
    }

    public HasherBloomFilter( Shape shape )
    {
        super( shape );
        this.hasher = new StaticHasher( EmptyIterator.emptyIterator(), shape );
    }

    @Override
    public LongBuffer getBits() {
        if (hasher.size() == 0)
        {
            return LongBuffer.allocate(0);
        }
        int n = (int) Math.ceil( hasher.getShape().getNumberOfBits() / Long.SIZE );
        LongBuffer result = LongBuffer.allocate( n );
        OfInt iter = hasher.getBits(hasher.getShape());
        while (iter.hasNext())
        {
            iter.forEachRemaining( (IntConsumer) idx -> {
                long buff = result.get( idx / Long.SIZE );
                int pwr = Math.floorMod(idx, Long.SIZE);
                long buffOffset = (long) Math.pow(2, pwr);
                buff |= buffOffset;
                result.put( idx / Long.SIZE, buff ) ;
            });
        }
        return result;
    }

    @Override
    public StaticHasher getHasher() {
        return hasher;
    }

    @Override
    public void merge(BloomFilter other) {
        merge( other.getShape(), other.getHasher());
    }

    @Override
    public void merge(Shape shape, Hasher hasher) {
        verifyShape( shape );
        IteratorChain<Integer> iter = new IteratorChain<Integer>( this.hasher.getBits(shape), hasher.getBits(shape));
        this.hasher = new StaticHasher( iter, this.hasher.getShape() );
    }

    @Override
    public int cardinality() {
        return hasher.size();
    }

    @Override
    public boolean contains(Shape shape, Hasher hasher) {
        verifyShape(shape);
        Set<Integer> set = new TreeSet<Integer>();
        hasher.getBits(shape).forEachRemaining( (IntConsumer) idx -> {
            set.add( idx );
        });
        OfInt iter = this.hasher.getBits(shape);
        while (iter.hasNext()) {
            int idx = iter.nextInt();
            set.remove( idx );
            if (set.isEmpty())
            {
                return true;
            }
        }
        return false;
    }

}
