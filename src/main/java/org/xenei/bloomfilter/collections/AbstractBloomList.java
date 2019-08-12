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
import java.util.TreeSet;
import java.util.function.Function;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A list of items of type T fronted by a bloom filter. 
 * 
 * All items in the list are represented by the gate filter.  The gate filter
 * is created by adding items to the list.
 * 
 * @param <T>
 *            The type of object indexed into the list.
 * 
 */
abstract class AbstractBloomList<T> {
	// the gate bloom filter.
	private BloomFilter gate;
	// the filter config for the gate.
	private final FilterConfig config;
	// a function for creating a bloom filter from a <T> object.
	private final Function<T,ProtoBloomFilter> func;
	// a sorted set of the items that we stored in this list.
	protected final SortedSet<DataWrapper<T>> buckets;
	
	// the number of bloom filters in the list.
	protected int size;

	/**
	 * Constructor.
	 * @param config The Filter Configuration.
	 * @param func A function for converting <T> objects into ProtoBloomFilters.
	 */
	public AbstractBloomList(FilterConfig config, Function<T,ProtoBloomFilter> func) {
		this.config = config;	
		this.func = func;
		this.buckets = new TreeSet<DataWrapper<T>>();
		this.size = 0;
		this.gate = new BloomFilter( config );
	}
	
	/**
	 * Add the object to this list.
	 * @param t
	 */
	public final void add(T t) {
		this.add( func.apply(t), t);
	}

	@Override
	public String toString() {
		return String.format("BloomList[ %s %s %s ]", size, gate, buckets);
	}

	/**
	 * Get the filter config for this list.
	 * @return the Filter Config.
	 */
	public FilterConfig getConfig() {
		return config;
	}
	/**
	 * Remove all items from this list.
	 */
	public void clear() {
		gate = new BloomFilter( config );
		this.buckets.clear();
		this.size = 0;
	}

	/**
	 * Get the gate filter.
	 * @return The Gating bloom filter.
	 */
	public BloomFilter getGate() {
		return gate;
	}

	/**
	 * Return true if this bloom filter is full.  A full bloom filter is one that 
	 * has the number of items defined in the gate filter configuration in the list.
	 * 
	 *  A full list is not prohibited or otherwise restricted from accepting more entries
	 *   but will be less efficient during searches.
	 *  
	 * @return true if the list is full.
	 */
	public boolean isFull() {
		return config.getNumberOfItems() <= size;
	}

	/**
	 * Get the number of bloom filter entries in this list.
	 * @return the number of bloom filter entries in the list.
	 */
	public int size() {
		return size;
	}

	/**
	 * Calculates the hamming distance from the gate filter to a filter.
	 * 
	 * @param f
	 *            The filter to calculate distance to.
	 * @return the distance
	 */
	public int distance(BloomFilter f) {
		return gate.distance(f);
	}

	/**
	 * Calculates the hamming distance to a proto filter.
	 * 
	 * @param pf
	 *            The proto filter to calculate distance to.
	 * @return the distance
	 */
	public int distance(ProtoBloomFilter pf) {
		return gate.distance(pf.create(config));
	}

	/**
	 * Returns true if the filter is found in this list.
	 * 
	 * @param filter The filter to look for.
	 * @return true if this list contains the filter.
	 */
	public boolean contains(BloomFilter filter) {
		return gate.inverseMatch(filter);
	}

	/**
	 * Returns true if pbf is found in this list. 
	 * 
	 * @param pbf the proto bloom filter to check.
	 * @return true if this list contains the filter.
	 */
	public boolean contains(ProtoBloomFilter pbf) {
		return gate.inverseMatch(pbf.create(config));
	}

	/**
	 * returns true if this gate is found in the pbf.
	 * 
	 * @param pbf the proto bloom filter to check.
	 * @return true if the proto bloom filter contains the gate for this list.
	 */
	public boolean inverseContains(ProtoBloomFilter pbf) {
		return gate.match(pbf.create(config));
	}

	/**
	 * returns true if this gate is found in the bf.
	 * 
	 * @param bf the bloom filter to check.
	 * @return  true if the bloom filter contains the gate for this list.
	 */
	public boolean inverseContains(BloomFilter bf) {
		return gate.match(bf);
	}

	/**
	 * Add the data item fronted by the proto bloom filter to this list.
	 * @param pbf the protobloom filter for the data item.
	 * @param t the date item
	 */
	abstract protected void add(ProtoBloomFilter pbf, T t);

	/**
	 * Get any exact matches for the proto bloom filter.  This is an exact match with the 
	 * bloom filter definition and may result in duplicates.
	 * @param f the proto bloom filter to match.
	 * @return an iterator over exact match data items.
	 */
	abstract public ExtendedIterator<T> getExactMatches(ProtoBloomFilter f);

	/**
	 * return true if there is an exact match.
	 * @param f the proto bloom filter to check for a match.
	 * @return true if there is an exact match, false otherwis.e
	 */
	public boolean hasExactMatch(ProtoBloomFilter f) {
		return getExactMatches(f).hasNext();
	}

	/**
	 * Get an iterator over all the data elements in the list.
	 * @return an iterator over the data elements.
	 */
	public ExtendedIterator<T> getCandidates() {
		return WrappedIterator.createIteratorIterator(
				WrappedIterator.create(buckets.iterator()).mapWith(new Function<DataWrapper<T>, Iterator<T>>() {

					@Override
					public Iterator<T> apply(DataWrapper<T> t) {
						return t.getData();
					}
				})

		);
	}

	/**
	 * Simply lists all the buckets to standard out.
	 * 
	 * Used for debugging.
	 */
	protected void dumpBuckets() {
		System.out.println(String.format("%s BUCKET DUMP >>>", this.getClass().getSimpleName()));
		Iterator<DataWrapper<T>> pwIter = buckets.iterator();
		while (pwIter.hasNext()) {
			DataWrapper<T> pw = pwIter.next();
			System.out.println(pw);
		}
		System.out.println(String.format("<<< %s BUCKET DUMP", this.getClass().getSimpleName()));
	}

	/**
	 * Get an iterator of candidates that match the proto bloom filter.  This is a bloom filter 
	 * match and may result in false positives.
	 * 
	 * @param pbf the bloom filter to match.
	 * @return an iterator over candidate data items.
	 */
	public ExtendedIterator<T> getCandidates(ProtoBloomFilter pbf) {
		BloomFilter f = pbf.create( config );
		if (contains(f)) {
			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter(config).inverseMatch(f))
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
	
	protected synchronized void merge( ProtoBloomFilter pbf )
	{
		this.gate = this.gate.merge( pbf.create(config) );
	}

}
