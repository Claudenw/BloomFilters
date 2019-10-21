/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloomfilter.collections;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.BloomFilterImpl;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A bloom list containing items of type T
 *
 * @param <T>
 */
public class BloomList<T> extends AbstractBloomCollection<T> {

	// a sorted set of the items that we stored in this list.
	protected final SortedSet<AbstractDataWrapper<T>> buckets;

	// the number of objects in the list.
	protected int size;

	/**
	 * Constructor.
	 * 
	 * @param config The Filter Configuration.
	 * @param func   A function for converting <T> objects into ProtoBloomFilters.
	 */
	public BloomList(FilterConfig config, Function<T, ProtoBloomFilter> func) {
		super(config, func);
		this.buckets = new TreeSet<AbstractDataWrapper<T>>();
		this.size = 0;
	}

	/**
	 * Get an iterator over all the data elements in the list.
	 * 
	 * @return an iterator over the data elements.
	 */
	@Override
	public ExtendedIterator<T> iterator() {
		return WrappedIterator.createIteratorIterator(
				WrappedIterator.create(buckets.iterator()).mapWith(new Function<AbstractDataWrapper<T>, Iterator<T>>() {

					@Override
					public Iterator<T> apply(AbstractDataWrapper<T> t) {
						return t.getData();
					}
				})

		);
	}

	@Override
	public boolean add(ProtoBloomFilter proto, T t) {
		merge(proto);
		AbstractDataWrapper<T> dw = new DataWrapper<T>(proto, t);
		// get the part of the set greater than or equal to dw.
		SortedSet<AbstractDataWrapper<T>> pwSet = buckets.tailSet(dw);

		if (!pwSet.isEmpty()) {
			AbstractDataWrapper<T> dw2 = pwSet.first();
			if (dw2.getProto().equals(proto)) {
				dw2.add(t);
				size++;
				return true;
			}
		}

		buckets.add(dw);
		size++;
		return true;
	}

	/**
	 * Clears the table.
	 */
	@Override
	protected void doClear() {
		buckets.clear();
		size = 0;
	}

	@Override
	public ExtendedIterator<T> getCandidates(ProtoBloomFilter proto) {
		BloomFilterImpl f = proto.create(getConfig());
		if (matches(f)) {
			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter(getConfig()).inverseMatch(f))
							.mapWith(new Function<AbstractDataWrapper<T>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(AbstractDataWrapper<T> t) {
									return t.getData();
								}
							})

			);
		} else {
			return NiceIterator.emptyIterator();
		}
	}

	@Override
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter proto) {
		BloomFilterImpl bf = proto.create(getConfig());
		if (matches(bf)) {

			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter(getConfig()).equals(bf))
							.mapWith(new Function<AbstractDataWrapper<T>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(AbstractDataWrapper<T> t) {
									return t.getData();
								}
							})

			);
		} else {
			return NiceIterator.emptyIterator();
		}
	}

	@Override
	public boolean remove(ProtoBloomFilter proto, T t) {
		BloomFilterImpl bf = proto.create(getConfig());
		boolean removed = false;
		if (matches(bf)) {
			Iterator<AbstractDataWrapper<T>> iter = buckets.iterator();
			while (iter.hasNext()) {
				AbstractDataWrapper<T> wrapper = iter.next();
				if (bf.match(wrapper.getFilter(getConfig()))) {
					removed = wrapper.remove(t);
					if (wrapper.size() == 0) {
						iter.remove();
						config.collectionStats.delete();
					}
				}
			}
		}
		return removed;
	}

	@Override
	public int size() {
		return size;
	}

	/**
	 * An wrapper that associates a proto bloom filter with one or more data items
	 * that have the same hash values.
	 *
	 * @param <T> The type of data to be wrapped.
	 */
	private static class DataWrapper<T> extends AbstractDataWrapper<T> {
		// the list of data items
		private List<T> data;

		/**
		 * Constructor.
		 * 
		 * @param proto The proto bloom filter to use.
		 * @param t     the data item to store.
		 */
		public DataWrapper(ProtoBloomFilter proto, T t) {
			super(proto);
			this.data = new ArrayList<T>();
			this.data.add(t);
		}

		/**
		 * Get an iterator over data from this wrapper.
		 * 
		 * @return an iterator over data.
		 */
		@Override
		public Iterator<T> getData() {
			return data.iterator();
		}

		/**
		 * the number of items that will be returned in the iterator.
		 * 
		 * @return The number of items fronted by this wrapper.
		 */
		@Override
		public int size() {
			return data.size();
		}

		/**
		 * Add an item to the wrapper.
		 * 
		 * @param t A data item that matches the filter.
		 */
		@Override
		public void add(T t) {
			data.add(t);
		}

		@Override
		public boolean remove(T t) {
			Iterator<T> tIter = getData();
			boolean removed = false;
			while (tIter.hasNext()) {
				T other = tIter.next();
				if (other.equals(t)) {
					tIter.remove();
					removed = true;
				}
			}
			return removed;
		}

	}
	
	public static class Factory<T> implements BloomCollectionFactory<T> {

		@Override
		public BloomList<T> getCollection(FilterConfig config, Function<T, ProtoBloomFilter> func)
				throws IOException {
			return new BloomList<T>(config, func);
		}
		
	}
}
