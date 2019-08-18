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
import java.util.List;
import java.util.function.Function;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A table is a collections of lists.
 * 
 * This table accepts duplicates.
 *
 * @param <T> the data type.
 */
public class BloomTable<T> extends AbstractBloomTable<T> {
	private static final int PAGE_SIZE = 10000;
	private static final int DEFAULT_BUCKETS = 5;
	private List<BloomCollection<T>> buckets;

	/**
	 * Default constructor. Creates a bloom table with a page size of 10000 and 5
	 * buckets.
	 *
	 * @param func The function to convert T instance to ProtoBloomFilters.
	 */
	public BloomTable(Function<T, ProtoBloomFilter> func) {
		this(DEFAULT_BUCKETS, PAGE_SIZE, func);
	}

	/**
	 * Create a table with the specified pageSize. the collision rate is calculated
	 * as 1/sqrt(pageSize)
	 * 
	 * @param buckets  the number of buckets to start with.
	 * @param pageSize the page size for each bucket.
	 * @param func     The function to convert T instance to ProtoBloomFilters.
	 */
	public BloomTable(int buckets, int pageSize, Function<T, ProtoBloomFilter> func) {
		this(buckets, new FilterConfig(pageSize, Double.valueOf(Math.ceil(Math.sqrt(pageSize))).intValue()), func);
	}

	/**
	 * Create a table with the specified parameters.
	 *
	 * @param buckets the number of buckets to start with.
	 * @param config  The Filter configuration.
	 * @param func    The function to convert T instance to ProtoBloomFilters.
	 */
	public BloomTable(int buckets, FilterConfig config, Function<T, ProtoBloomFilter> func) {
		this(buckets, new Config(config), func);
	}

	public BloomTable(int buckets, Config config, Function<T, ProtoBloomFilter> func) {
		super(config, func);
		this.buckets = new ArrayList<BloomCollection<T>>(buckets);
		this.buckets.add(new BloomList<T>(config.gateConfig, func));
		for (int i = 1; i < buckets; i++) {
			addEmptyBucket();
		}
	}

	@Override
	protected ExtendedIterator<BloomCollection<T>> getBuckets() {
		return WrappedIterator.create(buckets.iterator());
	}

	/**
	 * Clears the table.
	 */
	@Override
	protected void doClear() {
		buckets.clear();
	}

	@Override
	protected FilterConfig getBucketConfig() {
		return buckets.get(0).getConfig();
	}

	@Override
	protected void addEmptyBucket() {
		this.buckets.add(new BloomList<T>(getBucketConfig(), getFunc()));
	}

}
