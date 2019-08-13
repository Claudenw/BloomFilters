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

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * An wrapper that associates a proto bloom filter with one or more data items
 * that have the same hash values.
 *
 * @param <T> The type of data to be wrapped.
 */
public class DataWrapper<T> implements Comparable<DataWrapper<T>> {
	// the list of data items
	private List<T> data;
	/*
	 * the proto bloom filter. This proto filter is the same for all the items. That
	 * is each item generated the same proto filter.
	 */
	private ProtoBloomFilter proto;

	/*
	 * A soft reference to a calculated bloom filter and its configuration.
	 */
	private SoftReference<BloomFilterPair> bloomFilterPair;

	/**
	 * Constructor.
	 * 
	 * @param proto The proto bloom filter to use.
	 * @param t     the data item to store.
	 */
	public DataWrapper(ProtoBloomFilter proto, T t) {
		this.proto = proto;
		this.data = new ArrayList<T>();
		this.data.add(t);
		this.bloomFilterPair = null;
	}

	/**
	 * Get the proto filter from this wrapper.
	 * 
	 * @return the filter.
	 */
	public ProtoBloomFilter getFilter() {
		return proto;
	}

	/**
	 * get the bloom filter.
	 * 
	 * @param config the configuration for the filter.
	 * @return the bloom filter
	 */
	public BloomFilter getFilter(FilterConfig config) {
		BloomFilterPair bfp = (bloomFilterPair == null) ? null : bloomFilterPair.get();
		if (bfp != null && bfp.config.equals(config)) {
			return bfp.bloomFilter;
		}
		bfp = new BloomFilterPair(config);
		bloomFilterPair = new SoftReference<BloomFilterPair>(bfp);
		return bfp.bloomFilter;
	}

	/**
	 * Get an iterator over data from this wrapper.
	 * 
	 * @return an iterator over data.
	 */
	public Iterator<T> getData() {
		return data.iterator();
	}

	/**
	 * the number of items that will be returned in the iterator.
	 * 
	 * @return The number of items fronted by this wrapper.
	 */
	public int size() {
		return data.size();
	}

	/**
	 * Add an item to the wrapper.
	 * 
	 * @param t A data item that matches the filter.
	 */
	public void add(T t) {
		data.add(t);
	}

	@Override
	public int hashCode() {
		return proto.hashCode();
	}

	/**
	 * Datawrappers are equals if the proto bloom filters are equal. No test is made
	 * for container contents.
	 */
	@Override
	public boolean equals(Object o) {
		return (o instanceof DataWrapper<?>) ? ((DataWrapper<?>) o).proto.equals(proto) : false;
	}

	/**
	 * Compares proto bloom filters.
	 */
	@Override
	public int compareTo(DataWrapper<T> o) {
		return proto.compareTo(o.proto);
	}

	@Override
	public String toString() {
		return String.format("DataWrapper[ %s x %s [%s]", data.size(), proto, data);
	}

	/**
	 * Class to encapsulate the bloom filter and its config.
	 *
	 */
	private class BloomFilterPair {
		BloomFilter bloomFilter;
		FilterConfig config;

		BloomFilterPair(FilterConfig config) {
			this.config = config;
			this.bloomFilter = proto.create(config);
		}
	}
}
