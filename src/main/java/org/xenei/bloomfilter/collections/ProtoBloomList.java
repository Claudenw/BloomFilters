package org.xenei.bloomfilter.collections;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.function.Function;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

public class ProtoBloomList<T> extends AbstractBloomList<T, ProtoBloomFilter> {

	public ProtoBloomList(FilterConfig cfg) {
		super(cfg);
	}

	@Override
	protected DataWrapper<T, ProtoBloomFilter> makeDataWrapper(ProtoBloomFilter pbf, T t) {
		return new DataWrapper<T, ProtoBloomFilter>(pbf, t);
	}

	@Override
	public void add(ProtoBloomFilter pbf, T t) {
		getGate().add(pbf);
		DataWrapper<T, ProtoBloomFilter> pw = makeDataWrapper(pbf, t);
		SortedSet<DataWrapper<T, ProtoBloomFilter>> pwSet = buckets.tailSet(pw);
		if (!pwSet.isEmpty()) {
			DataWrapper<T, ProtoBloomFilter> pw2 = pwSet.first();
			if (pbf.equals(pw2.getFilter())) {
				pw2.add(t);
				return;
			}
		}
		buckets.add(pw);
		size++;
	}

	@Override
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter f) {
		if (contains(f)) {

			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter().equals(f))
							.mapWith(new Function<DataWrapper<T, ProtoBloomFilter>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(DataWrapper<T, ProtoBloomFilter> t) {
									return t.getData();
								}
							})

			);
		} else {
			return NiceIterator.emptyIterator();
		}
	}
}
