package org.xenei.bloomfilter.collections;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.function.Consumer;

import org.xenei.bloomfilter.BloomFilter;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

/**
 * The configuration of a bloom collection.
 *
 */
public class Config {
	public final FilterConfig gateConfig;
	private BloomFilter gate;
	public CollectionStats collectionStats;
	private Consumer<Config> changeNotification;

	public static Config read(DataInputStream stream) throws IOException {
		Config cfg = new Config(FilterConfig.read(stream));
		byte[] bitBuffer = new byte[cfg.gateConfig.getNumberOfBytes()];
		stream.read(bitBuffer);
		cfg.gate = new BloomFilter(BitSet.valueOf(bitBuffer));
		cfg.collectionStats = CollectionStats.read(stream);
		cfg.collectionStats.addConsumer(cfg.new Notifier());
		return cfg;
	}

	public void write(DataOutput stream) throws IOException {
		FilterConfig.write(gateConfig, stream);
		byte[] buff = new byte[gateConfig.getNumberOfBytes()];
		Arrays.fill(buff, (byte) 0);
		byte[] other = gate.asBitSet().toByteArray();
		System.arraycopy(other, 0, buff, 0, other.length);
		stream.write(buff);
		CollectionStats.write(collectionStats, stream);
	}

	public Config(FilterConfig gateConfig) {
		this.gateConfig = gateConfig;
		this.gate = new BloomFilter(gateConfig);
		this.collectionStats = new CollectionStats();
		collectionStats.addConsumer(new Notifier());
	}

	private void notifyChange() {
		if (changeNotification != null) {
			changeNotification.accept(this);
		}

	}

	public synchronized void addConsumer(Consumer<Config> consumer) {
		if (changeNotification == null) {
			changeNotification = consumer;
		} else {
			changeNotification.andThen(consumer);
		}
	}

	public BloomFilter getGate() {
		return gate;
	}

	public synchronized void merge(ProtoBloomFilter proto) {
		BloomFilter other = proto.create(gateConfig);
		if (!gate.inverseMatch(other)) {
			gate = gate.merge(other);
		}
		collectionStats.insert();
	}

	public int getStoredSize() {
		return FilterConfig.STORED_SIZE + CollectionStats.STORED_SIZE + gateConfig.getNumberOfBytes();
	}

	public void clear() {
		gate = new BloomFilter(gateConfig);
		collectionStats.clear();
	}

	private class Notifier implements Consumer<CollectionStats> {

		@Override
		public void accept(CollectionStats arg0) {
			notifyChange();
		}

	}
}