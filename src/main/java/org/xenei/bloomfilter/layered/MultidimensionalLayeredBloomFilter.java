package org.xenei.bloomfilter.layered;

import java.util.Arrays;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

public class MultidimensionalLayeredBloomFilter extends LayeredBloomFilter {
    private final long[][] map;
    private int updating;
    private boolean hitMax;
    private final Predicate<BloomFilter> extendCheck;
    
    public MultidimensionalLayeredBloomFilter(Shape shape, int maxDepth, Predicate<BloomFilter> extendCheck) {
        super(shape, maxDepth);
        this.extendCheck = extendCheck;
        map = new long[shape.getNumberOfBits()][BitMap.numberOfBitMaps(maxDepth)];
    }
    
    protected Predicate<BloomFilter> getMerge() {
        return this::doMerge;
    }

    protected boolean doMerge(BloomFilter bf) {
        int[] offset = {0};
        return bf.forEachBitMap( x-> {
            if (x != 0L) {
                int off = offset[0];
                long mask = BitMap.getLongBit(updating);
                int idx = BitMap.getLongIndex( updating );
                for (int i=0;i<Long.SIZE;i++) {
                    if ((x & (1L << i)) != 0)
                    {
                        map[off+i][idx] |= mask;
                    }
                }
            }
            offset[0]++;
            return true;
        });
    }

    protected boolean doContains(BloomFilter bf) {
        int[] offset = {0};
        return bf.forEachBitMap( x-> {
            if (x != 0L) {
                int off = offset[0];
                for (int i=0;i<Long.SIZE;i++) {
                    if ((x & (1L << i)) != 0)
                    {
                        if (Arrays.stream( map[off+i]).anyMatch(y -> y!=0)) {
                            break;
                        }
                        return false;
                    }
                }
            }
            offset[0]++;
            return true;
        });
    }
    
    @Override
    public BloomFilter copy() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clear() {
        for (int i=0;i<shape.getNumberOfBits();i++) {
            Arrays.fill( map[i], 0L);
        }
        updating = 0;
    }

    @Override
    public int cardinality() {
        int cardinality = 0;
        for (int i=0;i<shape.getNumberOfBits();i++) {
            if (Arrays.stream( map[i] ).anyMatch( x -> x!=0)) {
                cardinality++;
            }
        }
        return cardinality;
    }

    @Override
    public void next() {
        if (++updating >= maxDepth) {
            hitMax = true;
            updating = 0;
        }
    }

    @Override
    public int getDepth() {
        return hitMax ? maxDepth : updating;
    }

    @Override
    public void clear(int level) {
        long mask = ~BitMap.getLongBit(level);
        int pos = BitMap.getLongIndex(level);
        for (int i=0;i<shape.getNumberOfBits();i++) {
            map[i][pos] &= mask;
        }
    }

    @Override
    public boolean forEachBloomFilter(Predicate<BloomFilter> bloomFilterPredicate) {
        for (int i=0;i<getDepth();i++) {
            long mask = BitMap.getLongBit(i);
            int pos = BitMap.getLongIndex(i);
            IndexProducer ip = (x) -> {
                for (int bit=0; bit<shape.getNumberOfBits();bit++) {
                    if ((map[bit][pos] & mask) != 0) {
                        if (!x.test(bit)) {
                            return false;
                        }
                    }
                }
                return true;
            };
            BloomFilter bf = new SimpleBloomFilter(shape);
            bf.merge(ip);
            if (!bloomFilterPredicate.test( bf )) {
                return false;
            }
        }
        return true;
    }
    
    
    

}
