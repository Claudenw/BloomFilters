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
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A bloom list that uses proto bloom filters.
 *
 * @param <T> The data type to store.
 */
public class ProtoBloomList<T> extends AbstractBloomList<T, ProtoBloomFilter> {

	/**
	 * Constructor
	 * @param cfg the filter configuration for the gateway filter.
	 */
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
