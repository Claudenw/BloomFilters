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
	private List<BloomCollection<T>> buckets;
	private final BloomCollectionFactory<T> factory;
	
	/**
	 * Builder for BloomTables.
	 *
	 * @param <T> the data type being stored in the table.
	 */
	public static class Builder<T> {
		private static final int PAGE_SIZE = 10000;
		private static final int DEFAULT_BUCKETS = 5;
		
		private int pageCount = DEFAULT_BUCKETS;
		private int pageSize = PAGE_SIZE;
		private Function<T, ProtoBloomFilter> func;
		private Integer probability = null;
		private BloomCollectionFactory<T> factory;
		
		/**
		 * Set the minimum number of active pages.  This is the number of active containers that will hold objects.
		 * If a page fills up a new one will be created.
		 * <p>Default = 5</p>
		 * @param pageCount the number of buckets.
		 * @return this for chaining
		 */
		public Builder<T> setPageCount( int pageCount ) {
			if (pageCount < 1)
			{
				throw new IllegalArgumentException( "Page count must be greater than 0");
			}
			this.pageCount = pageCount;
			return this;
		}
		
		/**
		 * Set the page size.  This is the number of objects that are stored in a page.
		 * <p>Default = 10000</p>
		 * @param pageSize
		 * @return this for chaining
		 */
		public Builder<T> setPageSize( int pageSize )
		{
			if (pageSize <= 2)
			{
				throw new IllegalArgumentException( "PageSize must be greater than 2");
			}			
			this.pageSize = pageSize;
			return this;
		}

		/**
		 * Set the function for converting T objects to ProtoBloomFilters.
		 * @param func the function.
		 * @return this for chaining
		 */
		public Builder<T> setFunc( Function<T, ProtoBloomFilter> func )
		{
			this.func = func;
			return this;
		}

		/**
		 * Set the probability as 1/probability.  This is the requested probability of collision in the bloom filters.
		 * <p>Default = Square root of the pageSize (default page size yeilds 100)</p> 
		 * @param probability the probability.
		 * @return this for chaining.
		 */
		public Builder<T> setProbability( int probability) {
			if (probability <= 0)
			{
				throw new IllegalArgumentException( "probability must be greater than 0");
			}
			this.probability = probability;
			return this;
		}
		
		/**
		 * Use a FilterConfig to configure the probability and page size.
		 * @param config the FilterConfig to use the probability and pageSize from. 
		 * @return this for chaining.
		 */
		public Builder<T> setConfig( FilterConfig config ) {
			probability = config.getProbability();
			pageSize = config.getNumberOfItems();
			return this;
		}

		/**
		 * Set the BloomCollectionFactory.  This factory is used to create pages as needed.
		 * @param factory the factory to use to create pages.
		 * @return this for chaining.
		 */
		public Builder<T> setFactory( BloomCollectionFactory<T> factory )
		{
			this.factory = factory;
			return this;
		}
		
		/**
		 * Build a bloom table.
		 * @return A bloom table.
		 * @throws IOException on error.
		 */
		public BloomTable<T> build() throws IOException {
			if (func == null) {
				throw new  IllegalStateException("Function must be provided");
			}
			if (factory == null) {
				throw new  IllegalStateException("Factory must be provided");
			}
			if (probability == null)
			{
				probability = Double.valueOf(Math.ceil(Math.sqrt(pageSize))).intValue();
			}
			Config config = new Config( new FilterConfig( pageSize, probability ));
			
			return new BloomTable<T>(pageCount, config, func,  factory);

		}
	}

	private BloomTable(int buckets, Config config, Function<T, ProtoBloomFilter> func,  BloomCollectionFactory<T> factory) throws IOException {
		super(config, func);
		this.factory = factory;
		this.buckets = new ArrayList<BloomCollection<T>>(buckets);
		// must be one bucket.
		this.buckets.add( factory.getCollection(config.gateConfig, func));
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
	protected void addEmptyBucket() throws IOException {
		this.buckets.add( factory.getCollection(getBucketConfig(), getFunc()));
	}

}
