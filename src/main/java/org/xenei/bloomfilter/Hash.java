package org.xenei.bloomfilter;

import java.io.Serializable;
import java.util.BitSet;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A Bloom Filter hash calculation.
 * 
 * The hash is calculated as a 128-bit value. We store this as two 64-bit
 * values. We can then rapidly calculate the bloom filter for any given
 * configuration.
 *
 */
public class Hash implements Comparable<Hash>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5511398264986350484L;
	private long h1;
	private long h2;
	private transient Integer hashCode;

	public Hash(long h1, long h2) {
		this.h1 = h1;
		this.h2 = h2;
	}

	public BitSet populate(BitSet set, FilterConfig config) {
		if (set.size() < config.getNumberOfBits()) {
			throw new IllegalArgumentException(
					String.format("Bitset had %s bits, %s required", set.size(), config.getNumberOfBits()));
		}
		for (int i = 0; i < config.getNumberOfHashFunctions(); i++) {
			int j = Math.abs((int) ((h1 + (i * h2)) % config.getNumberOfBits()));
			set.set(j, true);
		}
		return set;
	}

	public long h1() {
		return h1;
	}

	public long h2() {
		return h2;
	}

	@Override
	public int compareTo(Hash other) {
		int result = Long.compare(h1, other.h1);
		if (result == 0) {
			result = Long.compare(h2, other.h2);
		}
		return result;
	}

	@Override
	public int hashCode() {
		if (hashCode == null) {
			hashCode = new HashCodeBuilder().append(h1).append(h2).build();
		}
		return hashCode.intValue();

	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Hash) {
			Hash other = (Hash) o;
			return h1 == other.h1 && h2 == other.h2;
		}
		return false;
	}

	@Override
	public String toString() {
		return String.format("Hash[ %s %s ]", Long.toHexString(h1), Long.toHexString(h2));
	}

}