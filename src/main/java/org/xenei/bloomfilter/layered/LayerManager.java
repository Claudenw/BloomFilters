package org.xenei.bloomfilter.layered;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;

public class LayerManager {

    public static class ExtendCheck {
        private ExtendCheck() {
        }

        /**
         * Advances the target once a merge has been performed.
         */
        public static final Predicate<LayerManager> ADVANCE_ON_POPULATED = lm -> {
            return !lm.filters.isEmpty() || !lm.filters.peekLast().forEachBitMap(y -> y == 0);
        };

        /**
         * Does not automatically advance the target. next() must be called directly to
         * perform the advance.
         */
        public static final Predicate<LayerManager> NEVER_ADVANCE = x -> false;

        /**
         * Calculates the estimated number of Bloom filters (n) that have been merged
         * into the target and compares that with the estimated maximum expected n based
         * on the shape. If the target is full then a new target is created.
         */
        public static final Predicate<LayerManager> ADVANCE_ON_CALCULATED_FULL = lm -> {
            if (lm.filters.isEmpty()){
                return false;
            }
            BloomFilter bf = lm.filters.peekLast();
            Shape s = bf.getShape();
            return s.estimateMaxN() <= s.estimateN(bf.cardinality());
        };
        
        public static Predicate<LayerManager> advanceOnCount(int breakAt) {
            return new Predicate<LayerManager>() {
                    int count = 0;

                    public boolean test(LayerManager filter) {
                        return ++count % breakAt == 0;
                    }
                };
        }
    }

    public static class FilterSupplier {
        private FilterSupplier() {
        }

        public static final Supplier<BloomFilter> simple(Shape shape) {
            return () -> new SimpleBloomFilter(shape);
        }
    }

    public static class Cleanup {
        private Cleanup() {
        }

        /**
         * Performs a cleanup when the number of filters in the list exceeds maxSize.
         *
         * @param maxSize the maximum number of filters for the list.
         * @return A Consumer for the LayerManager filterCleanup constructor argument.
         */
        public static final Consumer<LinkedList<BloomFilter>> onMaxSize(int maxSize) {
            return (ll) -> {
                while (ll.size() > maxSize) {
                    ll.removeFirst();
                }
            };
        }
    }

    private final LinkedList<BloomFilter> filters = new LinkedList<>();
    private final Consumer<LinkedList<BloomFilter>> filterCleanup;
    private final Predicate<LayerManager> extendCheck;
    private final Supplier<BloomFilter> filterSupplier;

    public LayerManager(Supplier<BloomFilter> filterSupplier, Predicate<LayerManager> extendCheck,
            Consumer<LinkedList<BloomFilter>> filterCleanup) {
        this.filterSupplier = filterSupplier;
        this.extendCheck = extendCheck;
        this.filterCleanup = filterCleanup;
        filters.add(this.filterSupplier.get());
    }

    public LayerManager copy() {
        LayerManager newMgr = new LayerManager(filterSupplier, extendCheck, filterCleanup);
        newMgr.filters.clear();
        newMgr.filters.addAll(filters);
        return newMgr;
    }

    /**
     * Advance to the next depth for subsequent merges.
     */
    private final void next() {
        if (!filters.isEmpty() && filters.getLast().cardinality() == 0) {
            filters.removeLast();
        }
        this.filterCleanup.accept(filters);
        filters.add(this.filterSupplier.get());
    }

    public final int getDepth() {
        return filters.size();
    }

    public final BloomFilter get(int depth) {
        if (depth < 0 || depth >= filters.size()) {
            throw new NoSuchElementException(String.format("Depth must be in the range [0,%s)", filters.size()));
        }
        return filters.get(depth);
    }

    public final BloomFilter target() {
        if (extendCheck.test(this)) {
            next();
        }
        return filters.peekLast();
    }

    public final void clear() {
        filters.clear();
        next();
    }

    public final void clear(int level) {
        filters.remove(level);
    }

    public boolean forEachBloomFilter(Predicate<BloomFilter> bloomFilterPredicate) {
        for (BloomFilter bf : filters) {
            if (!bloomFilterPredicate.test(bf)) {
                return false;
            }
        }
        return true;
    }
}
