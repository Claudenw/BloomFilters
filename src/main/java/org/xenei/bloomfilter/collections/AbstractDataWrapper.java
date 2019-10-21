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
import java.util.Iterator;
import org.xenei.bloomfilter.BloomFilterImpl;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * An wrapper that associates a proto bloom filter with one or more data items
 * that have the same hash values.
 *
 * @param <T> The type of data to be wrapped.
 */
public abstract class AbstractDataWrapper<T> implements Comparable<AbstractDataWrapper<T>> {

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
	 */
	public AbstractDataWrapper(ProtoBloomFilter proto) {
		this.proto = proto;
		this.bloomFilterPair = null;
	}

	protected AbstractDataWrapper() {
		this(null);
	}

	protected void setProtoBloomFilter(ProtoBloomFilter proto) {
		this.proto = proto;
	}

	/**
	 * Get the proto filter from this wrapper.
	 * 
	 * @return the filter.
	 */
	public final ProtoBloomFilter getProto() {
		return proto;
	}

	/**
	 * get the bloom filter.
	 * 
	 * @param config the configuration for the filter.
	 * @return the bloom filter
	 */
	public final BloomFilterImpl getFilter(FilterConfig config) {
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
	abstract public Iterator<T> getData();

	/**
	 * the number of items that will be returned in the iterator.
	 * 
	 * @return The number of items fronted by this wrapper.
	 */
	abstract public int size();

	/**
	 * Add an item to the wrapper.
	 * 
	 * @param t A data item that matches the filter.
	 */
	abstract public void add(T t);

	abstract public boolean remove(T t);

	@Override
	public final int hashCode() {
		return proto.hashCode();
	}

	/**
	 * Datawrappers are equals if the proto bloom filters are equal. No test is made
	 * for container contents.
	 */
	@Override
	public final boolean equals(Object o) {
		return (o instanceof AbstractDataWrapper<?>) ? ((AbstractDataWrapper<?>) o).proto.equals(proto) : false;
	}

	/**
	 * Compares proto bloom filters.
	 */
	@Override
	public final int compareTo(AbstractDataWrapper<T> o) {
		return proto.compareTo(o.proto);
	}

	/**
	 * Class to encapsulate the bloom filter and its config.
	 *
	 */
	private class BloomFilterPair {
		BloomFilterImpl bloomFilter;
		FilterConfig config;

		BloomFilterPair(FilterConfig config) {
			this.config = config;
			this.bloomFilter = proto.create(config);
		}
	}
}
