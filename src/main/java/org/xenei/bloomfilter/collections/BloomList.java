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

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A bloom list containing items of type T
 *
 * @param <T>
 */
public class BloomList<T> extends AbstractBloomCollection<T> {

	// a sorted set of the items that we stored in this list.
	protected final SortedSet<DataWrapper<T>> buckets;

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
		this.buckets = new TreeSet<DataWrapper<T>>();
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
				WrappedIterator.create(buckets.iterator()).mapWith(new Function<DataWrapper<T>, Iterator<T>>() {

					@Override
					public Iterator<T> apply(DataWrapper<T> t) {
						return t.getData();
					}
				})

		);
	}

	@Override
	public boolean add(ProtoBloomFilter pbf, T t) {
		merge(pbf);
		DataWrapper<T> dw = new DataWrapper<T>(pbf, t);
		// get the part of the set greater than or equal to dw.
		SortedSet<DataWrapper<T>> pwSet = buckets.tailSet(dw);

		if (!pwSet.isEmpty()) {
			DataWrapper<T> dw2 = pwSet.first();
			if (dw2.getFilter().equals(pbf)) {
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
	public ExtendedIterator<T> getCandidates(ProtoBloomFilter pbf) {
		BloomFilter f = pbf.create(getConfig());
		if (matches(f)) {
			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter(getConfig()).inverseMatch(f))
							.mapWith(new Function<DataWrapper<T>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(DataWrapper<T> t) {
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
		BloomFilter bf = proto.create(getConfig());
		if (matches(bf)) {

			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter(getConfig()).equals(bf))
							.mapWith(new Function<DataWrapper<T>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(DataWrapper<T> t) {
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
		BloomFilter bf = proto.create(getConfig());
		boolean removed = false;
		if (matches(bf)) {
			Iterator<DataWrapper<T>> iter = buckets.iterator();
			while (iter.hasNext())
			{
				DataWrapper<T> wrapper = iter.next();
				if (bf.match( wrapper.getFilter( getConfig() ) ))
				{
					Iterator<T> tIter = wrapper.getData();
					while (tIter.hasNext())
					{
						T other = tIter.next();
						if (other.equals( t ))
						{
							tIter.remove();
							size--;
							removed = true;
						}
					}
					if ( ! wrapper.getData().hasNext())
					{
						iter.remove();
						collectionStats.delete();
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

}
