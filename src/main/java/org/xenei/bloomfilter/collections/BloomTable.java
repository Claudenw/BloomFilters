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
import java.util.function.Function;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A table that contains one or more BloomLists.
 * 
 * This table accepts duplicates.
 *
 * @param <T> the data type.
 */
public abstract class BloomTable<T> {
	private static final int PAGE_SIZE = 10000;
	private BloomList<ProtoBloomList<T>> headers;
	private FilterConfig childConfig;

	/**
	 * Create a table with the specified item size.
	 * @param itemSize The number of items expected in each bloom list.
	 */
	public BloomTable(int itemSize) {
		this(PAGE_SIZE, itemSize);
	}

	/**
	 * Create a table with the specified page and item size. 
	 * @param pageSize The number of items that are expected to be inserted into this table.
	 * @param itemSize the number of items that are expected to be places on each list within the table.
	 */
	public BloomTable(int pageSize, int itemSize) {
		this.childConfig = new FilterConfig(itemSize, pageSize);
		this.headers = new BloomList<ProtoBloomList<T>>(new FilterConfig(pageSize, pageSize));
	}

	/**
	 * Create the ProtoBloomFilter for the object.
	 * 
	 * @param t
	 *            The object to create the proto filter for.
	 * @return The constructed ProtoBloomFilter
	 */
	abstract protected ProtoBloomFilter createProto(T t);

	/**
	 * Clears ths table.
	 */
	public void clear() {
		headers.clear();
	}

	/**
	 * Get the gating bloom filter.  This is the filter defined by the page size and that tracks
	 * all items places in the table. 
	 * @return The gating bloom filter.
	 */
	public BloomFilter getGate() {
		return headers.getGate();
	}

	/**
	 * Put an item in the table.
	 * @param t The item to put in the table.
	 */
	public void put(T t) {
		ProtoBloomFilter pbf = createProto(t);
		ProtoBloomList<T> pbl = new ProtoBloomList<T>(childConfig);
		pbl.add(pbf, t);
		headers.add(pbf, pbl);
	}

	/**
	 * Get the iterator of candidates that might match the data item.
	 * @param t The data item to match.
	 * @return an iterator of possible matches.
	 */
	public ExtendedIterator<T> getCandidates(T t) {
		final ProtoBloomFilter pbf = createProto(t);

		Iterator<Iterator<T>> iter = headers.getCandidates(pbf).mapWith(new Function<ProtoBloomList<T>, Iterator<T>>() {

			@Override
			public Iterator<T> apply(ProtoBloomList<T> bl) {
				return bl.getCandidates(pbf);
			}
		});

		return WrappedIterator.createIteratorIterator(iter);
	}

}
