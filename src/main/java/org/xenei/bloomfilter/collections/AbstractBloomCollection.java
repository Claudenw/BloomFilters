package org.xenei.bloomfilter.collections;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.function.Function;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.bloomfilter.BloomFilterImpl;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

public abstract class AbstractBloomCollection<T> extends AbstractCollection<T> implements BloomCollection<T> {
	protected final Config config;
	// a function for creating a bloom filter from a <T> object.
	private final Function<T, ProtoBloomFilter> func;

	/**
	 * Constructor.
	 * 
	 * @param config The Filter Configuration.
	 * @param func   A function for converting <T> objects into ProtoBloomFilters.
	 */
	protected AbstractBloomCollection(FilterConfig config, Function<T, ProtoBloomFilter> func) {
		this(new Config(config), func);
	}

	/**
	 * Constructor.
	 * 
	 * @param config The Filter Configuration.
	 * @param func   A function for converting <T> objects into ProtoBloomFilters.
	 */
	protected AbstractBloomCollection(Config config, Function<T, ProtoBloomFilter> func) {
		this.config = config;
		this.func = func;

	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean contains(Object o) {
		return contains(func.apply((T) o), (T) o);
	}

	@Override
	public final boolean contains(ProtoBloomFilter proto, T t) {
		return getCandidates(proto).filterKeep(t2 -> t2.equals(t)).hasNext();
	}

	/**
	 * Clears the table.
	 */
	@Override
	public final void clear() {
		doClear();
		config.clear();
	}

	/**
	 * Subclasses should override this to perform clear methods.
	 */
	protected void doClear() {
		super.clear();
	}

	@Override
	public final boolean add(T o) {
		return add(func.apply(o), o);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean remove(Object o) {
		if (o == null) {
			throw new NullPointerException();
		}
		return remove(func.apply((T) o), (T) o);
	}

	@Override
	public final boolean isEmpty() {
		return this.config.getGate().getHammingWeight() == 0;
	}

	@Override
	public final FilterConfig getConfig() {
		return this.config.gateConfig;
	}

	@Override
	public final Function<T, ProtoBloomFilter> getFunc() {
		return func;
	}

	@Override
	public final BloomFilterImpl getGate() {
		return this.config.getGate();
	}

	@Override
	public final boolean isFull() {
		return this.config.gateConfig.getNumberOfItems() <= size();
	}

	@Override
	public final int distance(BloomFilterImpl f) {
		return this.config.getGate().distance(f);
	}

	@Override
	public final int distance(ProtoBloomFilter pf) {
		return this.config.getGate().distance(pf.create(this.config.gateConfig));
	}

	@Override
	public final boolean matches(BloomFilterImpl filter) {
		return this.config.getGate().inverseMatch(filter);
	}

	@Override
	public final boolean matches(ProtoBloomFilter pbf) {
		return this.config.getGate().inverseMatch(pbf.create(this.config.gateConfig));
	}

	@Override
	public final boolean inverseMatch(ProtoBloomFilter pbf) {
		return this.config.getGate().inverseMatch(pbf.create(this.config.gateConfig));
	}

	@Override
	public final boolean inverseMatch(BloomFilterImpl bf) {
		return this.config.getGate().inverseMatch(bf);
	}

	@Override
	public final CollectionStats getStats() {
		return this.config.collectionStats;
	}

	public final int getFilterCount() {
		return CollectionStats.asInt(this.config.collectionStats.getFilterCount());
	}

	protected final void merge(ProtoBloomFilter proto) {
		this.config.merge(proto);
	}

	/**
	 * Get an iterator of candidates that match the proto bloom filter. This is a
	 * bloom filter match and may result in false positives.
	 * 
	 * @param pbf the bloom filter to match.
	 * @return an iterator over candidate data items.
	 */
	public final ExtendedIterator<T> getCandidates(T t) {
		return getCandidates(func.apply(t));
	}

}
