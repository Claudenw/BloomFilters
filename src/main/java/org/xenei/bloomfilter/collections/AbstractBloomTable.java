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
import java.util.Iterator;
import java.util.function.Function;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.BloomFilterImpl;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.Utils;

/**
 * A table is a collections of lists.
 * 
 * This table accepts duplicates.
 *
 * @param <T> the data type.
 */
public abstract class AbstractBloomTable<T> extends AbstractBloomCollection<T> {
	private static final int PAGE_SIZE = 10000;

	/**
	 * Default constructor. Creates a bloom table with a page size of 10000 and 5
	 * buckets.
	 *
	 * @param func The function to convert T instance to ProtoBloomFilters.
	 */
	public AbstractBloomTable(Function<T, ProtoBloomFilter> func) {
		this(PAGE_SIZE, func);
	}

	/**
	 * Create a table with the specified pageSize. the collision rate is calculated
	 * as 1/sqrt(pageSize)
	 * 
	 * @param buckets  the number of buckets to start with.
	 * @param pageSize the page size for each bucket.
	 * @param func     The function to convert T instance to ProtoBloomFilters.
	 */
	public AbstractBloomTable(int pageSize, Function<T, ProtoBloomFilter> func) {
		this(new FilterConfig(pageSize, Double.valueOf(Math.ceil(Math.sqrt(pageSize))).intValue()), func);
	}

	/**
	 * Create a table with the specified parameters.
	 *
	 * @param buckets the number of buckets to start with.
	 * @param config  The Filter configuration.
	 * @param func    The function to convert T instance to ProtoBloomFilters.
	 */
	public AbstractBloomTable(FilterConfig config, Function<T, ProtoBloomFilter> func) {
		this(new Config(config), func);
	}

	public AbstractBloomTable(Config config, Function<T, ProtoBloomFilter> func) {
		super(config, func);
	}

	protected abstract ExtendedIterator<BloomCollection<T>> getBuckets();

	protected abstract FilterConfig getBucketConfig();

	protected abstract void addEmptyBucket() throws IOException;

	/**
	 * Put an item in the table.
	 * 
	 * @param t The item to put in the table.
	 */
	@Override
	public boolean add(ProtoBloomFilter proto, T t) {
		// use the bucket config not the master config.
		merge(proto);
		BloomFilterImpl bf = proto.create(getBucketConfig());
		int minDist = bf.getHammingWeight();
		BloomCollection<T> minList = null;
		Iterator<BloomCollection<T>> iter = getBuckets();
		while (iter.hasNext()) {
			BloomCollection<T> lst = iter.next();
			if (!lst.isFull()) {
				int thisDist = lst.distance(bf);
				if (minList == null || thisDist < minDist) {
					minDist = thisDist;
					minList = lst;
				}
			}
		}
		if (minList == null) {
			throw new IllegalStateException("no bucket space");
		}

		try {
			return minList.add(proto, t);
		} finally {
			// entry is full so add another bucket.
			if (minList.isFull()) {
				try {
					addEmptyBucket();
				} catch (IOException e) {
					throw new IllegalStateException( e );
				}
			}
		}
	}

	@Override
	public ExtendedIterator<T> iterator() {
		return WrappedIterator.createIteratorIterator(getBuckets().mapWith(bl -> bl.iterator()));
	}

	@Override
	public int size() {
		return Utils.iteratorToStream(getBuckets()).mapToInt(bl -> bl.size()).sum();
	}

	@Override
	public boolean remove(ProtoBloomFilter proto, T t) {
		boolean removed = false;
		Iterator<BloomCollection<T>> iter = getBuckets();
		while (iter.hasNext()) {
			BloomCollection<T> bloomCollection = iter.next();
			long deleteCount = bloomCollection.getStats().getDeleteCount();
			if (bloomCollection.remove(proto, t)) {
				long count = bloomCollection.getStats().getDeleteCount() - deleteCount;
				getStats().delete(count);
				removed = true;
			}
		}
		return removed;
	}

	@Override
	public ExtendedIterator<T> getCandidates(ProtoBloomFilter pbf) {
		return WrappedIterator.createIteratorIterator(getBuckets().mapWith(bc -> bc.getCandidates(pbf)));
	}

	@Override
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter proto) {
		return WrappedIterator.createIteratorIterator(getBuckets().mapWith(bc -> getExactMatches(proto)));
	}

}
