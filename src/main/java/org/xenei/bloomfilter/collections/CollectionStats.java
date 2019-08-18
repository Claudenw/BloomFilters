package org.xenei.bloomfilter.collections;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Consumer;

public class CollectionStats {

	private long filterInserts;
	private long filterDeletes;
	private Consumer<CollectionStats> changeNotification;

	public static final int STORED_SIZE = 2 * Long.BYTES;

	public static void write(CollectionStats cs, DataOutput oos) throws IOException {
		oos.writeLong(cs.getInsertCount());
		oos.writeLong(cs.getDeleteCount());
	}

	public static CollectionStats read(DataInput ois) throws IOException {
		CollectionStats cs = new CollectionStats();
		cs.filterInserts = ois.readLong();
		cs.filterDeletes = ois.readLong();
		return cs;
	}

	public CollectionStats() {
		this(null);
	}

	public synchronized void addConsumer(Consumer<CollectionStats> consumer) {
		if (changeNotification == null) {
			changeNotification = consumer;
		} else {
			changeNotification.andThen(consumer);
		}
	}

	public CollectionStats(Consumer<CollectionStats> changeNotification) {
		filterInserts = 0;
		filterDeletes = 0;
		this.changeNotification = changeNotification;
	}

	private void notifyChange() {
		if (changeNotification != null) {
			changeNotification.accept(this);
		}
	}

	public void clear() {
		filterInserts = 0;
		filterDeletes = 0;
		notifyChange();
	}

	public static int asInt(long l) {
		return (l > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (l < Integer.MIN_VALUE) ? Integer.MIN_VALUE : (int) l;
	}

	public long getFilterCount() {
		return filterInserts - filterDeletes;
	}

	public double badHitFactor() {
		if (filterInserts == 0 || filterDeletes == 0) {
			return 0.0;
		}
		double ratio = filterDeletes / (filterInserts * 1.0);
		return ratio;
	}

	public void insert() {
		if (filterInserts < Long.MAX_VALUE) {
			filterInserts++;
		}
		notifyChange();
	}

	public void delete() {
		delete(1);
	}

	public void delete(long count) {
		if (filterDeletes < Long.MAX_VALUE - count) {
			filterDeletes += count;
		} else {
			filterDeletes = Long.MAX_VALUE;
		}
		notifyChange();
	}

	public long getInsertCount() {
		return filterInserts;
	}

	public long getDeleteCount() {
		return filterDeletes;
	}

	public long getTxnCount() {
		return filterInserts + filterDeletes;
	}

}
