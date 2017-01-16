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
import org.xenei.bloomfilter.BloomFilterI;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * A list of items of type T fronted by a bloom filter. All items in this list
 * match the gate bloom filter.
 * 
 * The gate is built by adding objects to the list
 * 
 * @param <T>
 *            The type of object indexed into the list.
 * 
 */
abstract class AbstractBloomList<T, F extends BloomFilterI<?>> {
	// the gate bloom filter.
	private BloomFilter gate;
	
	// a sorted set of the items that we stored in this list.
	protected final SortedSet<DataWrapper<T, F>> buckets;
	
	// the number of bloom filters in the list.
	protected int size;

	/**
	 * Constructor
	 * @param cfg The filter configuration for the gate filter.
	 */
	public AbstractBloomList(FilterConfig cfg) {
		this.gate = new BloomFilter(cfg);
		this.buckets = new TreeSet<DataWrapper<T, F>>();
	}

	@Override
	public String toString() {
		return String.format("BloomList[ %s %s %s ]", size, gate, buckets);
	}

	/**
	 * Remove all items from this list.
	 */
	public void clear() {
		gate.clear();
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
		return gate.getConfig().getNumberOfItems() <= size;
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
		return gate.distance(pf);
	}

	/**
	 * checks f & gate == f
	 * 
	 * @param f The filter to look for.
	 * @return true if this list contains the filter.
	 */
	public boolean contains(BloomFilter f) {
		return gate.inverseMatch(f);
	}

	/**
	 * checks pbf & gate == pbf
	 * 
	 * @param pbf the proto bloom filter to check.
	 * @return true if this list contains the filter.
	 */
	public boolean contains(ProtoBloomFilter pbf) {
		return gate.inverseMatch(pbf);
	}

	/**
	 * checks gate & pbf == gate
	 * 
	 * @param pbf the proto bloom filter to check.
	 * @return true if the proto bloom filter contains the gate for this list.
	 */
	public boolean inverseContains(ProtoBloomFilter pbf) {
		return gate.match(pbf);
	}

	/**
	 * checks gate & bf == gate
	 * 
	 * @param bf the bloom filter to check.
	 * @return  true if the bloom filter contains the gate for this list.
	 */
	public boolean inverseContains(BloomFilter bf) {
		return gate.match(bf);
	}

	/**
	 * Create the data wrapper instance from a protobloomfilter and the data element.
	 * @param pbf the Proto bloom filter for the datawrapper
	 * @param t The data item.
	 * @return The data wrapper.
	 */
	abstract protected DataWrapper<T, F> makeDataWrapper(ProtoBloomFilter pbf, T t);

	/**
	 * Add the data item fronted by the proto bloom filter to this list.
	 * @param pbf the protobloom filter for the data item.
	 * @param t the date item
	 */
	abstract public void add(ProtoBloomFilter pbf, T t);

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
				WrappedIterator.create(buckets.iterator()).mapWith(new Function<DataWrapper<T, F>, Iterator<T>>() {

					@Override
					public Iterator<T> apply(DataWrapper<T, F> t) {
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
		Iterator<DataWrapper<T, F>> pwIter = buckets.iterator();
		while (pwIter.hasNext()) {
			DataWrapper<T, F> pw = pwIter.next();
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
		BloomFilter f = pbf.create(gate.getConfig());
		if (contains(f)) {
			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter().inverseMatch(f))
							.mapWith(new Function<DataWrapper<T, F>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(DataWrapper<T, F> t) {
									return t.getData();
								}
							})

			);
		} else {
			return NiceIterator.emptyIterator();
		}
	}

}
