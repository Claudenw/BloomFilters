package org.xenei.bloomfilter.collections;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.function.Function;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

public class BloomList<T> extends AbstractBloomList<T, BloomFilter> {

	public BloomList(FilterConfig cfg) {
		super(cfg);
	}

	@Override
	protected DataWrapper<T, BloomFilter> makeDataWrapper(ProtoBloomFilter pbf, T t) {
		return new DataWrapper<T, BloomFilter>(pbf.create(getGate().getConfig()), t);
	}

	@Override
	public void add(ProtoBloomFilter pbf, T t) {
		getGate().add(pbf);
		DataWrapper<T, BloomFilter> dw = makeDataWrapper(pbf, t);
		SortedSet<DataWrapper<T, BloomFilter>> pwSet = buckets.tailSet(dw);
		if (!pwSet.isEmpty() && addClosestMatch(pwSet, dw.getFilter(), pbf, t)) {

			return;
		}

		buckets.add(dw);
		size++;
	}

	private boolean addClosestMatch(SortedSet<DataWrapper<T, BloomFilter>> set, BloomFilter bf, ProtoBloomFilter pbf,
			T t) {

		// find the closest
		DataWrapper<T, BloomFilter> target = null;
		int dist = Integer.MAX_VALUE;
		boolean logMatch = false;

		Iterator<DataWrapper<T, BloomFilter>> iter = set.iterator();
		while (iter.hasNext() && !logMatch) {
			DataWrapper<T, BloomFilter> bl = iter.next();
			System.out.println(bl);

			if (bl.getFilter().getLog() == bf.getLog()) {
				logMatch = true;
				target = bl;
			} else {

				int d = bl.getFilter().distance(bf);
				if (d < dist) {
					target = bl;
					dist = d;
				}
			}
		}

		/*
		 * if the closest is further away than a new one would be construct a
		 * new one
		 */
		if (!logMatch && dist >= bf.getHammingWeight()) {
			return false;
		}

		// make sure it is sorted correctly
		buckets.remove(target);
		target.add(t);
		buckets.add(target);
		return true;
	}

	@Override
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter f) {
		BloomFilter bf = f.create(getGate().getConfig());
		if (contains(bf)) {

			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter().isEqual(bf))
							.mapWith(new Function<DataWrapper<T, BloomFilter>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(DataWrapper<T, BloomFilter> t) {
									return t.getData();
								}
							})

			);
		} else {
			return NiceIterator.emptyIterator();
		}
	}
}
