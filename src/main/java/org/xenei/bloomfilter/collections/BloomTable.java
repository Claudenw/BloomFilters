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
import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A table is a collections of lists.
 * 
 * This table accepts duplicates.
 *
 * @param <T> the data type.
 */
public class BloomTable<T> {
	private static final int PAGE_SIZE = 10000;
	private static final int DEFAULT_BUCKETS = 5;
	private List<BloomList<T>> buckets;
	private BloomFilter gate;
	private FilterConfig gateConfig;
	private final Function<T,ProtoBloomFilter> func;

	/**
	 * Default constructor.  
	 * Creates a bloom table with a page size of 10000 and 5 buckets.
	 *
	 * @param func The function to convert T instance to ProtoBloomFilters.
	 */
	public BloomTable(Function<T,ProtoBloomFilter> func)
	{
		this( DEFAULT_BUCKETS, PAGE_SIZE, func );
	}
	/**
	 * Create a table with the specified pageSize.
	 * the collision rate is calculated as 1/sqrt(pageSize)
	 * 
	 * @param buckets the number of buckets to start with.
	 * @param pageSize the page size for each bucket.
	 * @param func The function to convert T instance to ProtoBloomFilters.
	 */
	public BloomTable(int buckets, int pageSize, Function<T,ProtoBloomFilter> func) {
		this( buckets, new FilterConfig(pageSize, Double.valueOf(Math.ceil(Math.sqrt(pageSize))).intValue()), func);
	}

	/**
	 * Create a table with the specified parameters.
	 *
	 * @param buckets the number of buckets to start with.
	 * @param config The Filter configuration.
	 * @param func The function to convert T instance to ProtoBloomFilters.
	 */
	public BloomTable(int buckets, FilterConfig config, Function<T,ProtoBloomFilter> func) {
		this.gateConfig = config;
		this.buckets = new ArrayList<BloomList<T>>(buckets);
		for (int i=0;i<buckets;i++)
		{
			this.buckets.add(new BloomList<T>(config, func));
		}
		this.func = func;		
	}

	/**
	 * Clears the table.
	 */
	public void clear() {
		buckets.clear();
		gate = new BloomFilter(gateConfig);
	}

	/**
	 * Get the gating bloom filter.  This is the filter defined by the page size and that tracks
	 * all items places in the table. 
	 * @return The gating bloom filter.
	 */
	public BloomFilter getGate() {
		return gate;
	}

	/**
	 * Put an item in the table.
	 * @param t The item to put in the table.
	 */
	public void put(T t) {
		ProtoBloomFilter pbf = func.apply(t);
		BloomFilter bf = pbf.create(buckets.get(0).getConfig());
		int minDist = bf.getHammingWeight();
		BloomList<T> minList = null;
		for (BloomList<T> lst : buckets)
		{
			if ( !lst.isFull())
			{
				int thisDist = lst.distance( bf );
				if (minList == null || thisDist<minDist)
				{
					minDist = thisDist;
					minList = lst;
				}
			}
		}
		if (minList == null) {
			throw new IllegalStateException( "no bucket space");
		}
		

		minList.add(pbf, t);
		
		// entry is full so add another bucket.
		if (minList.isFull())
		{
			buckets.add(new BloomList<T>(minList.getConfig(), func));
		}
	}

	/**
	 * Get the iterator of candidates that might match the data item.
	 * @param t The data item to match.
	 * @return an iterator of possible matches.
	 */
	public ExtendedIterator<T> getCandidates(T t) {
		final ProtoBloomFilter pbf = func.apply(t);

		return WrappedIterator.createIteratorIterator( 
				WrappedIterator.create(buckets.iterator())
				.mapWith( bl -> bl.getCandidates(pbf)));
		
		
	}

}
