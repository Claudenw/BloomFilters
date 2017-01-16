package org.xenei.bloomfilter;

public interface BloomFilterI<T> extends Comparable<T> {

	public boolean match(final BloomFilter other);

	public boolean inverseMatch(final BloomFilter other);
}
