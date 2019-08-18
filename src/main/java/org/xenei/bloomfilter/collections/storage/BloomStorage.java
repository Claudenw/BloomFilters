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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.blockstorage.FileStorage;
import org.xenei.blockstorage.Storage;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;
import org.xenei.bloomfilter.collections.BloomCollection;
import org.xenei.bloomfilter.collections.Config;
import org.xenei.bloomfilter.collections.AbstractBloomTable;
import org.xenei.spanbuffer.SpanBuffer;
import org.xenei.spanbuffer.streams.SpanBufferOutputStream;

/**
 * A bloom list containing items of type T
 *
 * @param <T>
 */
public class BloomStorage<T> extends AbstractBloomTable<T> {

	// the number of objects in the list.
	protected int size;

	protected Storage storage;

	private List<BloomStorageList<T>> buckets;

	private FilterConfig bucketConfig;

	private final Consumer<BloomStorageList<T>> listener = new Consumer<BloomStorageList<T>>() {

		@Override
		public void accept(BloomStorageList<T> arg0) {
			write(config);
		}
	};

	public static <T> BloomStorage<T> create(int partitions, String fileName, Function<T, ProtoBloomFilter> func)
			throws IOException {

		Storage storage = new FileStorage(fileName);
		SpanBuffer baseData = storage.getFirstRecord();
		InputStream is = baseData.getInputStream();
		DataInputStream dis = new DataInputStream(is);
		Config cfg = Config.read(dis);

		return new BloomStorage<T>(partitions, storage, cfg.gateConfig, func);

	}

	public BloomStorage(int partitions, Storage storage, FilterConfig config, Function<T, ProtoBloomFilter> func)
			throws IOException {
		super(read(storage, config), func);
		this.storage = storage;
		this.bucketConfig = super.config.gateConfig;
		read(partitions);

		super.config.addConsumer(new Consumer<Config>() {

			@Override
			public void accept(Config arg0) {
				write(arg0);
			}

		});
	}

	private static Config read(Storage storage, FilterConfig config) throws IOException {
		try {
			SpanBuffer baseData = storage.getFirstRecord();
			InputStream is = baseData.getInputStream();
			DataInputStream dis = new DataInputStream(is);
			return Config.read(dis);
		} catch (EOFException e) {
			return new Config(config);
		}
	}

	private void read(int partitions) {
		try {
			SpanBuffer baseData = storage.getFirstRecord();
			InputStream is = baseData.getInputStream();
			List<Long> lst = null;
			try (DataInputStream dis = new DataInputStream(is)) {
				Config.read(dis); // skip this
				int size = dis.readInt();
				lst = new ArrayList<Long>();
				for (int i = 0; i < size; i++) {
					lst.add(dis.readLong());
				}
			} catch (IOException e) {
				// do nothing.
			}
			if (lst != null && lst.size() > 0) {
				buckets = lst.stream().map(pos -> {
					try {
						return new BloomStorageList<T>(storage, pos, bucketConfig, getFunc());
					} catch (ClassNotFoundException | IOException e) {
						throw new IllegalStateException(e);
					}
				}).peek(bsl -> bsl.addConsumer(listener)).collect(Collectors.toList());
			} else {
				buckets = new ArrayList<BloomStorageList<T>>();
			}
			if (buckets.size() < partitions) {
				while (buckets.size() < partitions) {
					addEmptyBucket();
				}
				write(super.config);
			}

		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private void write(Config cfg) {
		SpanBufferOutputStream sbos = new SpanBufferOutputStream();
		try (DataOutputStream dos = new DataOutputStream(sbos);) {
			cfg.write(dos);
			List<Long> lst = buckets.stream().map(BloomStorageList::getPosition).collect(Collectors.toList());
			Collections.sort(lst);
			dos.writeInt(lst.size());
			for (Long l : lst) {
				dos.writeLong(l);
			}
			dos.flush();
			dos.close();
			storage.setFirstRecord(sbos.getSpanBuffer());
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected ExtendedIterator<BloomCollection<T>> getBuckets() {
		return WrappedIterator.create(buckets.iterator()).mapWith(BloomCollection.class::cast);
	}

	@Override
	protected FilterConfig getBucketConfig() {
		return bucketConfig;
	}

	@Override
	protected void addEmptyBucket() {
		try {
			BloomStorageList<T> storageList = new BloomStorageList<T>(storage, 0, getBucketConfig(), getFunc());
			storageList.addConsumer(listener);
			this.buckets.add(storageList);
		} catch (ClassNotFoundException | IOException e) {
			throw new IllegalStateException(e);
		}
	}

}
