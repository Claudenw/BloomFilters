package org.xenei.bloomfilter.stable;

import java.util.Objects;
import java.util.Random;
import java.util.function.IntPredicate;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.commons.collections4.bloomfilter.Shape;

/**
 * Generate sudo random integers using combinatorial hashing as described by
 * <a href="https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf">Krisch and
 * Mitzenmacher</a> using the enhanced double hashing technique described in the
 * wikipedia article <a href=
 * "https://en.wikipedia.org/wiki/Double_hashing#Enhanced_double_hashing">Double
 * Hashing</a> and random seeds for the initial value and the increment.
 */
public class FastPseudoRandomInt implements Hasher {
    private volatile long index;
    private volatile long increment;
    private volatile long count;

    public FastPseudoRandomInt() {
        Random r = new Random();
        index = r.nextLong();
        increment = r.nextLong();
        count = 1;
    }

    /**
     * Generates a sudo random number in the range [0,limit).
     *
     * @param limit The limit for the index value (exclusive).
     * @return a pseudo random integer.
     */
    public int nextInt(int limit) {
        int idx = BitMap.mod(index, limit);
        // Update index and handle wrapping
        index -= increment;

        // Incorporate the counter into the increment to create a
        // tetrahedral number additional term, and handle wrapping.
        increment -= count++;
        return idx;
    }

    @Override
    public IndexProducer indices(final Shape shape) {
        Objects.requireNonNull(shape, "shape");

        return new IndexProducer() {

            @Override
            public boolean forEachIndex(final IntPredicate consumer) {
                Objects.requireNonNull(consumer, "consumer");
                final int bits = shape.getNumberOfBits();
                final int k = shape.getNumberOfHashFunctions();
                if (k > bits) {
                    for (int j = k; j > 0;) {
                        // handle k > bits
                        final int block = Math.min(j, bits);
                        j -= block;
                        for (int i = 0; i < block; i++) {
                            if (!consumer.test(nextInt(bits))) {
                                return false;
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < k; i++) {
                        if (!consumer.test(nextInt(bits))) {
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public int[] asIndexArray() {
                final int[] result = new int[shape.getNumberOfHashFunctions()];
                final int[] idx = new int[1];

                // This method needs to return duplicate indices

                forEachIndex(i -> {
                    result[idx[0]++] = i;
                    return true;
                });
                return result;
            }
        };
    }
}
