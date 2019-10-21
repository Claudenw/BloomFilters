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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.bloomfilter.BloomFilterImpl;
import org.xenei.bloomfilter.Hash;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.span.LongSpan;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.streams.SpanBufferInputStream;

/**
 * A bloom list containing items of type T
 *
 * @param <T>
 */
public class BloomFile<T> extends AbstractBloomCollection<T> {

	// the number of objects in the list.
	protected int size;

	private SortedSet<Bucket<T>> buckets;

	private boolean dirty;

	public static <T> BloomFile<T> create(String fileName, FilterConfig config, Function<T, ProtoBloomFilter> func)
			throws IOException {

		SpanBuffer buffer = null;
		try {
			buffer = org.xenei.spanbuffer.Factory.wrapFile(fileName);
		} catch (FileNotFoundException expected) {
			buffer = org.xenei.spanbuffer.Factory.EMPTY;
		}
		if (buffer.getLength() == 0) {
			return new BloomFile<T>(config, func);
		} else {
			try (SpanBufferInputStream sbis = buffer.getInputStream();
					DataInputStream dis = new DataInputStream(sbis)) {
				return new BloomFile<T>(func, sbis, dis);
			}
		}
	}

	private BloomFile(FilterConfig config, Function<T, ProtoBloomFilter> func) {
		super(config, func);
		this.dirty = false;
		this.buckets = new TreeSet<Bucket<T>>();
	}

	/**
	 * Constructor.
	 * 
	 * @param config The Filter Configuration.
	 * @param func   A function for converting <T> objects into ProtoBloomFilters.
	 */
	private BloomFile(Function<T, ProtoBloomFilter> func, SpanBufferInputStream sbis, DataInputStream dis)
			throws IOException {
		super(Config.read(dis), func);
		this.dirty = false;
		this.size = dis.readInt();
		int bucketCount = dis.readInt();
		this.buckets = new TreeSet<Bucket<T>>();

		for (int i = 0; i < bucketCount; i++) {
			int hashCount = dis.readInt();
			int objCount = dis.readInt();
			int objlength = dis.readInt();
			Set<Hash> hashes = new TreeSet<Hash>();
			for (int j = 0; j < hashCount; j++) {
				long h1 = dis.readLong();
				long h2 = dis.readLong();
				hashes.add(new Hash(h1, h2));
			}
			Bucket<T> bucket = new Bucket<T>(new ProtoBloomFilter(hashes));
			bucket.objCount = objCount;
			bucket.data = sbis.getSpanBuffer().cut(sbis.getBytesRead()).head(objlength);
			sbis.skip(objlength);
			buckets.add(bucket);
		}
	}

	public boolean isDirty() {
		return dirty;
	}

	public void write(DataOutputStream oos) throws IOException {
		config.write(oos);
		oos.writeInt(this.size);
		oos.writeInt(this.buckets.size());
		for (Bucket<T> bucket : buckets) {
			oos.writeInt(bucket.getProto().getHashes().size());
			oos.writeInt(bucket.objCount);
			oos.writeInt((int) bucket.data.getLength());
			for (Hash hash : bucket.getProto().getHashes()) {
				oos.writeLong(hash.h1());
				oos.writeLong(hash.h2());
			}
			IOUtils.copy(bucket.data.getInputStream(), oos);
		}
		dirty = false;
	}

	/**
	 * Get an iterator over all the data elements in the list.
	 * 
	 * @return an iterator over the data elements.
	 */
	@Override
	public ExtendedIterator<T> iterator() {
		return WrappedIterator.createIteratorIterator(
				WrappedIterator.create(buckets.iterator()).mapWith(new Function<Bucket<T>, Iterator<T>>() {

					@Override
					public Iterator<T> apply(Bucket<T> bucket) {
						return bucket.getData();
					}
				}));
	}

	@Override
	public boolean add(ProtoBloomFilter proto, T t) {
		merge(proto);

		Bucket<T> dw = new Bucket<T>(proto);
		// get the part of the set greater than or equal to dw.
		SortedSet<Bucket<T>> pwSet = buckets.tailSet(dw);

		if (!pwSet.isEmpty()) {
			AbstractDataWrapper<T> dw2 = pwSet.first();
			if (dw2.getProto().equals(proto)) {
				dw2.add(t);
				size++;
				dirty = true;
				return true;
			}
		}
		dw.add(t);
		buckets.add(dw);
		size++;
		dirty = true;
		return true;
	}

	/**
	 * Clears the table.
	 */
	@Override
	protected void doClear() {
		buckets.clear();
		size = 0;
		dirty = true;
	}

	@Override
	public ExtendedIterator<T> getCandidates(ProtoBloomFilter proto) {
		BloomFilterImpl f = proto.create(getConfig());
		if (matches(f)) {
			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter(getConfig()).inverseMatch(f))
							.mapWith(new Function<Bucket<T>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(Bucket<T> t) {
									return t.getData();
								}
							})

			);
		} else {
			return NiceIterator.emptyIterator();
		}
	}

	@Override
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter proto) {
		BloomFilterImpl bf = proto.create(getConfig());
		if (matches(bf)) {

			return WrappedIterator.createIteratorIterator(
					WrappedIterator.create(buckets.iterator()).filterKeep(b -> b.getFilter(getConfig()).equals(bf))
							.mapWith(new Function<Bucket<T>, Iterator<T>>() {

								@Override
								public Iterator<T> apply(Bucket<T> t) {
									return t.getData();
								}
							})

			);
		} else {
			return NiceIterator.emptyIterator();
		}
	}

	@Override
	public boolean remove(ProtoBloomFilter proto, T t) {
		BloomFilterImpl bf = proto.create(getConfig());
		boolean removed = false;
		if (matches(bf)) {
			Iterator<Bucket<T>> iter = buckets.iterator();
			while (iter.hasNext()) {
				AbstractDataWrapper<T> wrapper = iter.next();
				if (bf.match(wrapper.getFilter(getConfig()))) {
					removed = wrapper.remove(t);
					if (wrapper.size() == 0) {
						iter.remove();
						config.collectionStats.delete();
					}
				}
			}
		}
		dirty |= removed;
		return removed;
	}

	@Override
	public int size() {
		return size;
	}

	private static class Bucket<T> extends AbstractDataWrapper<T> {
		private int objCount;
		private SpanBuffer data;

		public Bucket(ProtoBloomFilter proto) {
			super(proto);
			data = org.xenei.spanbuffer.Factory.EMPTY;
		}

		@Override
		public Iterator<T> getData() {
			return new BucketIterator();
		}

		@Override
		public int size() {
			return objCount;
		}

		@Override
		public void add(T t) {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();

					ObjectOutputStream oos = new ObjectOutputStream(baos);) {
				oos.writeObject(t);
				oos.flush();
				SpanBuffer sb = org.xenei.spanbuffer.Factory.wrap(baos.toByteArray());
				data = org.xenei.spanbuffer.Factory.merge(data, sb);
				objCount++;
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public boolean remove(T t) {
			try (SpanBufferInputStream sbis = data.getInputStream()) {
				List<LongSpan> spansToRemove = new ArrayList<LongSpan>();
				ObjectInputStream ois = new ObjectInputStream(sbis);
				try {
					while (true) {
						long start = sbis.getBytesRead();
						Object other = ois.readObject();
						if (t.equals(other)) {
							spansToRemove.add(LongSpan.fromEnd(start, sbis.getBytesRead()));
						}
					}
				} catch (EOFException expected) {
					if (spansToRemove.size() == 0) {
						return false;
					}
					objCount -= spansToRemove.size();
					if (objCount <= 0) {
						objCount = 0;
						data = org.xenei.spanbuffer.Factory.EMPTY;
					} else {
						List<SpanBuffer> buffers = new ArrayList<SpanBuffer>();
						long nxtStart = 0;
						for (LongSpan s : spansToRemove) {
							if (buffers.isEmpty()) {
								buffers.add(data.head(s.getOffset()));
							} else {
								buffers.add(data.tail(nxtStart).head(s.getOffset()));
							}
							nxtStart = s.getOffset() + s.getLength();
						}
						if (nxtStart < data.getLength()) {
							buffers.add(data.safeTail(nxtStart));
						}
						data = org.xenei.spanbuffer.Factory.merge(buffers.iterator());
					}
					return true;
				}

			} catch (IOException | ClassNotFoundException e) {
				throw new IllegalStateException(e);
			}
		}

		private class BucketIterator implements Iterator<T> {
			private int next;
			private ObjectInputStream ois;

			BucketIterator() {
				next = 0;
				ois = null;
			}

			@Override
			public boolean hasNext() {
				return next < objCount;
			}

			@SuppressWarnings("unchecked")
			@Override
			public T next() {
				if (hasNext()) {
					next++;
					if (ois == null) {
						try {
							ois = new ObjectInputStream(data.getInputStream());
						} catch (IOException e) {
							throw new IllegalStateException(e);
						}

					}
					try {
						return (T) ois.readObject();
					} catch (ClassNotFoundException | IOException e) {
						throw new IllegalStateException(e);
					}

				} else {
					throw new NoSuchElementException();
				}
			}

		}

	}
	
	public static class Factory<T> implements BloomCollectionFactory<T> {

		private File baseDir;
		
		public Factory( File baseDir ) {
			boolean ok = (baseDir.exists() && baseDir.isDirectory()) ||
					(!baseDir.exists());
			if (!ok) {
				throw new IllegalArgumentException("baseDir must either exist as a directory or not exist at all");
			}
			if (!baseDir.exists()) {
				baseDir.mkdirs();
			}
			this.baseDir = baseDir;
		}
		
		@Override
		public BloomFile<T> getCollection(FilterConfig config, Function<T, ProtoBloomFilter> func) throws IOException {
			return create(String.format( "%s/%s",baseDir.getAbsolutePath(), UUID.randomUUID()), config, func);
		}
		
	}
}
