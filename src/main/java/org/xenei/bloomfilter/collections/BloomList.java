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
public class BloomList<T> extends AbstractBloomList<T> {

	/**
	 * Create a bloom list.
	 * @param cfg The filter configuration for the gating filter.
	 */
	public BloomList(FilterConfig cfg, Function<T,ProtoBloomFilter> func) {
		super(cfg, func);
	}

	@Override
	public void add(ProtoBloomFilter pbf, T t) {		
		merge(pbf);
		DataWrapper<T> dw = new DataWrapper<T>(pbf, t);
		// get the part of the set greater than or equal to dw.
		SortedSet<DataWrapper<T>> pwSet = buckets.tailSet(dw);
		
		if (!pwSet.isEmpty())
		{
			DataWrapper<T> dw2 = pwSet.first();
			if (dw2.getFilter().equals( pbf ))
			{
				dw2.add( t );
				return;
			}
		}
		
		buckets.add(dw);
		size++;
	}

	
	@Override
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter f) {
		BloomFilter bf = f.create(getConfig());
		if (contains(bf)) {

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
}
