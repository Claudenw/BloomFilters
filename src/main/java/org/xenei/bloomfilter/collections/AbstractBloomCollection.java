package org.xenei.bloomfilter.collections;

import java.util.AbstractCollection;
import java.util.function.Function;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

public abstract class AbstractBloomCollection<T> extends AbstractCollection<T> implements BloomCollection<T> {
	// the gate bloom filter.
	private BloomFilter gate;
	// the filter config for the gate.
	private final FilterConfig gateConfig;
	// a function for creating a bloom filter from a <T> object.
	private final Function<T, ProtoBloomFilter> func;

	private int filterCount;

	/**
	 * Constructor.
	 * 
	 * @param config The Filter Configuration.
	 * @param func   A function for converting <T> objects into ProtoBloomFilters.
	 */
	protected AbstractBloomCollection(FilterConfig config, Function<T, ProtoBloomFilter> func) {
		this.gateConfig = config;
		this.func = func;
		this.gate = new BloomFilter(config);
		this.filterCount = 0;

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
		this.gate = new BloomFilter(gateConfig);
		this.filterCount = 0;
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
		return this.gate.getHammingWeight() == 0;
	}

	@Override
	public final FilterConfig getConfig() {
		return gateConfig;
	}

	@Override
	public final Function<T, ProtoBloomFilter> getFunc() {
		return func;
	}

	@Override
	public final BloomFilter getGate() {
		return gate;
	}

	@Override
	public final boolean isFull() {
		return gateConfig.getNumberOfItems() <= size();
	}

	@Override
	public final int distance(BloomFilter f) {
		return gate.distance(f);
	}

	@Override
	public final int distance(ProtoBloomFilter pf) {
		return gate.distance(pf.create(gateConfig));
	}

	@Override
	public final boolean matches(BloomFilter filter) {
		return gate.inverseMatch(filter);
	}

	@Override
	public final boolean matches(ProtoBloomFilter pbf) {
		return gate.inverseMatch(pbf.create(gateConfig));
	}

	@Override
	public final boolean inverseMatch(ProtoBloomFilter pbf) {
		return gate.inverseMatch(pbf.create(gateConfig));
	}

	@Override
	public final boolean inverseMatch(BloomFilter bf) {
		return gate.inverseMatch(bf);
	}

	public int getFilterCount() {
		return filterCount;
	}

	protected synchronized void merge(ProtoBloomFilter pbf) {
		BloomFilter other = pbf.create(gateConfig);
		if (gate.inverseMatch(other)) {
			return;
		}
		this.gate = this.gate.merge(pbf.create(gateConfig));
		this.filterCount++;
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
