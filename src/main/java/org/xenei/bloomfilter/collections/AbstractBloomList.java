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
 * A list of itmes of type T fronted by a bloom filter. All items in this list
 * match the gate bloom filter.
 * 
 * The gate is built by adding objects to the list
 * 
 * @param <T>
 *            The type of object indexed into the list.
 * 
 */
abstract class AbstractBloomList<T, F extends BloomFilterI<?>> {
	private BloomFilter gate;
	protected final SortedSet<DataWrapper<T, F>> buckets;
	protected int size;

	public AbstractBloomList(FilterConfig cfg) {
		this.gate = new BloomFilter(cfg);
		this.buckets = new TreeSet<DataWrapper<T, F>>();
	}

	@Override
	public String toString() {
		return String.format("BloomList[ %s %s %s ]", size, gate, buckets);
	}

	public void clear() {
		gate.clear();
		this.buckets.clear();
		this.size = 0;
	}

	public BloomFilter getGate() {
		return gate;
	}

	public boolean isFull() {
		return gate.getConfig().getNumberOfItems() <= size;
	}

	public int size() {
		return size;
	}

	/**
	 * Calculates the hamming distance to a filter.
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
	 * @param f
	 * @return
	 */
	public boolean contains(BloomFilter f) {
		return gate.inverseMatch(f);
	}

	/**
	 * checks pbf & gate == pbf
	 * 
	 * @param pbf
	 * @return
	 */
	public boolean contains(ProtoBloomFilter pbf) {
		return gate.inverseMatch(pbf);
	}

	/**
	 * checks gate & pbf == gate
	 * 
	 * @param pbf
	 * @return
	 */
	public boolean inverseContains(ProtoBloomFilter pbf) {
		return gate.match(pbf);
	}

	/**
	 * checks gate & bf == gate
	 * 
	 * @param bf
	 * @return
	 */
	public boolean inverseContains(BloomFilter bf) {
		return gate.match(bf);
	}

	abstract protected DataWrapper<T, F> makeDataWrapper(ProtoBloomFilter pbf, T t);

	abstract public void add(ProtoBloomFilter pbf, T t);

	abstract public ExtendedIterator<T> getExactMatches(ProtoBloomFilter f);

	public boolean hasExactMatch(ProtoBloomFilter f) {
		return getExactMatches(f).hasNext();
	}

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

	protected void dumpBuckets() {
		System.out.println(String.format("%s BUCKET DUMP >>>", this.getClass().getSimpleName()));
		Iterator<DataWrapper<T, F>> pwIter = buckets.iterator();
		while (pwIter.hasNext()) {
			DataWrapper<T, F> pw = pwIter.next();
			System.out.println(pw);
		}
		System.out.println(String.format("<<< %s BUCKET DUMP", this.getClass().getSimpleName()));
	}

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
