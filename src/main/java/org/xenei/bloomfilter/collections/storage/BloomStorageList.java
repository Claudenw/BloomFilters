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
package org.xenei.bloomfilter.collections.storage;

import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.blockstorage.Storage;
import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.collections.AbstractBloomCollection;

/**
 * A bloom list containing items of type T for which the data are read/written
 * to storage.
 * 
 * This block stores the bloom filters and storage pointers for the lists of
 * data.
 *
 * @param <T>
 */
public class BloomStorageList<T> extends AbstractBloomCollection<T> {

	// the number of objects in the list.
	private int size;

	// the storage engine
	private final Storage storage;

	// the list of bloom protos
	private TreeSet<DataPointer> protos;

	// the storage position where
	private long position;

	private Consumer<BloomStorageList<T>> changeNotification;

	/**
	 * Constructor.
	 * 
	 * @param config The Filter Configuration.
	 * @param func   A function for converting <T> objects into ProtoBloomFilters.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public BloomStorageList(Storage storage, long position, FilterConfig config, Function<T, ProtoBloomFilter> func)
			throws IOException, ClassNotFoundException {
		super(config, func);
		this.storage = storage;
		this.position = position;
		this.protos = (TreeSet<DataPointer>) storage.readObject(position);
		this.size = protos.stream().mapToInt(DataPointer::size).sum();
	}

	private void notifyChange() {
		if (changeNotification != null) {
			changeNotification.accept(this);
		}

	}

	public synchronized void addConsumer(Consumer<BloomStorageList<T>> consumer) {
		if (changeNotification == null) {
			changeNotification = consumer;
		} else {
			changeNotification.andThen(consumer);
		}
	}

	public long getPosition() {
		return position;
	}

	/**
	 * Get an iterator over all the data elements in the list.
	 * 
	 * @return an iterator over the data elements.
	 */
	@Override
	public ExtendedIterator<T> iterator() {
		return WrappedIterator
				.createIteratorIterator(WrappedIterator.create(protos.iterator()).mapWith(DataPointer::iterator));
	}

	@Override
	public boolean add(ProtoBloomFilter proto, T t) {
		merge(proto);
		DataPointer dataPointer = new DataPointer(proto);
		// get the part of the set greater than or equal to dw.
		SortedSet<DataPointer> dpSet = protos.tailSet(dataPointer);

		if (!dpSet.isEmpty()) {
			DataPointer pd = dpSet.first();
			if (pd.getProto().equals(proto)) {
				pd.add(t);
				size++;
				return true;
			}
		}

		// add the new proto here.
		protos.add(dataPointer);
		dataPointer.add(t);
		size++;
		writeProtos();
		return true;
	}

	private void writeProtos() {
		try {
			if (position > 0) {
				storage.write(position, protos);
			} else {
				position = storage.append(protos);
				notifyChange();
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

	}

	/**
	 * Clears the table.
	 */
	@Override
	protected void doClear() {
		for (DataPointer dp : protos) {
			try {
				storage.delete(dp.position);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		protos.clear();
		size = 0;
		writeProtos();
	}

	@Override
	public ExtendedIterator<T> getCandidates(ProtoBloomFilter proto) {
		BloomFilter f = proto.create(getConfig());
		if (matches(f)) {
			return WrappedIterator.createIteratorIterator(WrappedIterator.create(protos.iterator())
					.filterKeep(dp -> dp.getFilter(getConfig()).inverseMatch(f)).mapWith(DataPointer::iterator));
		} else {
			return NiceIterator.emptyIterator();
		}
	}

	@Override
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter proto) {
		BloomFilter bf = proto.create(getConfig());
		if (matches(bf)) {

			return WrappedIterator.createIteratorIterator(WrappedIterator.create(protos.iterator())
					.filterKeep(dp -> dp.getFilter(getConfig()).equals(bf)).mapWith(DataPointer::iterator));
		} else {
			return NiceIterator.emptyIterator();
		}
	}

	@Override
	public boolean remove(ProtoBloomFilter proto, T t) {
		BloomFilter bf = proto.create(getConfig());
		if (matches(bf)) {
			// use int[] to make final for accumulator below.
			final long[] removedCount = new long[1];
			List<DataPointer> removeList = protos.stream().filter(dp -> bf.match(dp.getFilter(getConfig())))
					.peek(dp -> {
						dp.remove(t);
						removedCount[0]++;
					}).collect(Collectors.toList());
			if (removeList.size() > 0) {
				protos.removeAll(removeList);
				writeProtos();
			}
			if (removedCount[0] > 0) {
				config.collectionStats.delete(removedCount[0]);
			}
			return removedCount[0] > 0;
		}
		return false;
	}

	@Override
	public int size() {
		return size;
	}

	private class DataPointer extends AbstractSet<T> implements Serializable, Comparable<DataPointer> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3702072879638140311L;
		private final ProtoBloomFilter proto;
		private long position;
		private transient SoftReference<HashSet<T>> dataRef;
		private int size;

		DataPointer(ProtoBloomFilter proto) {
			this.proto = proto;
			this.dataRef = null;
			this.position = 0;
			this.size = 0;
		}

		public ProtoBloomFilter getProto() {
			return proto;
		}

		public BloomFilter getFilter(FilterConfig config) {
			return proto.create(config);
		}

		@SuppressWarnings("unchecked")
		private HashSet<T> getData() {
			HashSet<T> data = (dataRef == null) ? null : dataRef.get();
			if (data == null) {
				if (position == 0) {
					return (HashSet<T>) Collections.EMPTY_SET;
				}
				try {
					data = (HashSet<T>) storage.readObject(position);
					dataRef = new SoftReference<HashSet<T>>(data);
				} catch (IOException | ClassNotFoundException e) {
					throw new IllegalStateException(e);
				}
			}
			return data;
		}

		@Override
		public boolean add(T t) {
			HashSet<T> data = getData();
			if (data.add(t)) {
				size++;
				try {
					if (position == 0) {
						position = storage.append(data);
					}
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
				return true;
			}
			return false;
		}

		@Override
		public boolean remove(Object o) {
			HashSet<T> data = getData();
			if (data.remove(o)) {
				size--;
				try {
					if (size <= 0) {
						storage.delete(position);
						position = 0;
						size = 0;
					} else {
						storage.write(position, data);
					}
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
				return true;
			}
			return false;
		}

		@Override
		public Iterator<T> iterator() {
			return getData().iterator();
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public int compareTo(DataPointer other) {
			return proto.compareTo(other.proto);
		}

	}
}
