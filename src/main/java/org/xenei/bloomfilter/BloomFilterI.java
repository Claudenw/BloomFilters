package org.xenei.bloomfilter;

/**
 * A bloom filter interface used to define common methods for both BloomFilter and ProtoBloomFilter.
 *
 */
public interface BloomFilterI<T> extends Comparable<T> {

	/**
	 * Return true if this & other == this
	 * 
	 * @param other
	 *            the other bloom filter to match.
	 * @return true if they match.
	 */
	public boolean match(final BloomFilter other);

	/**
	 * Return true if other & this == other
	 * 
	 * @param other
	 *            the other bloom filter to match.
	 * @return true if they match.
	 */
	public boolean inverseMatch(final BloomFilter other);
}
