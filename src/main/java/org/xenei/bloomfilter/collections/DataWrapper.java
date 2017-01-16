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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.xenei.bloomfilter.BloomFilterI;

/**
 * An wrapper that associates a bloom filter or proto bloom filter with a data item.
 *
 * @param <T> The type of data to be wrapped.
 * @param <F> either BloomFilter or ProtoBloomFilter.
 */
public class DataWrapper<T, F extends BloomFilterI> implements Comparable<DataWrapper<T, F>> {
	private List<T> data;
	private F filter;

	/**
	 * Constructor.
	 * @param f The bloom filter to use.
	 * @param t the data item to store.
	 */
	public DataWrapper(F f, T t) {
		this.filter = f;
		this.data = Arrays.asList(t);
	}

	/**
	 * Get the filter from this wrapper.
	 * @return the filter.
	 */
	public F getFilter() {
		return filter;
	}

	/**
	 * Get an iterator over matching data from this wrapper.
	 * @return an iterator over data that matches the filter. 
	 */
	public Iterator<T> getData() {
		return data.iterator();
	}

	/**
	 * the number of items that will be returned in the iterator.
	 * @return The number of items fronted by this wrapper.
	 */
	public int size() {
		return data.size();
	}

	/**
	 * Add an item to the wrapper.
	 * @param t A data item that matches the filter.
	 */
	public void add(T t) {
		if (data.size() == 1) {
			T t1 = data.get(0);
			data = new ArrayList<T>(2);
			data.add(t1);
		}
		data.add(t);
	}

	
	@Override
	public int hashCode() {
		return filter.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return (o instanceof DataWrapper<?, ?>) ? ((DataWrapper<?, ?>) o).filter.equals(filter) : false;
	}

	@Override
	public int compareTo(DataWrapper<T, F> o) {
		return filter.compareTo(o.filter);
	}

	@Override
	public String toString() {
		return String.format("DataWrapper[ %s x %s [%s]", data.size(), filter, data);
	}

}
