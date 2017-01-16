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
public class BloomList<T> extends AbstractBloomList<T, BloomFilter> {

	/**
	 * Create a bloom list.
	 * @param cfg The filter configuration for the gating filter.
	 */
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

	/*
	 * Adds the item to the closest match if possible.
	 * 
	 *  returns true if the item was added, false otherwise.
	 */
	private boolean addClosestMatch(SortedSet<DataWrapper<T, BloomFilter>> set, BloomFilter bf, ProtoBloomFilter pbf,
			T t) {

		// find the closest
		DataWrapper<T, BloomFilter> target = null;
		int dist = Integer.MAX_VALUE;
		boolean logMatch = false;

		Iterator<DataWrapper<T, BloomFilter>> iter = set.iterator();
		while (iter.hasNext() && !logMatch) {
			DataWrapper<T, BloomFilter> bl = iter.next();

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
