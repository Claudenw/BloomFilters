/*
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
package org.xenei.bloomfilter;

import java.util.BitSet;

/**
 * A bloom filter definition.
 *
 */
public class BloomFilter implements BloomFilterI<BloomFilter> {

	// the bitset we are using
	private final BitSet bitSet;

	// the hamming value once we have calculated it.
	private transient Integer hamming;

	private transient Double logValue;

	private final FilterConfig config;

	/**
	 * Constructor
	 */
	public BloomFilter(FilterConfig config, BitSet bitSet) {
		this.config = config;
		this.bitSet = bitSet;
		this.hamming = null;
	}

	public BloomFilter(FilterConfig config) {
		this(config, new BitSet(config.getNumberOfBits()));
	}

	public void clear() {
		bitSet.clear();
		hamming = null;
		logValue = null;
	}

	public FilterConfig getConfig() {
		return config;
	}

	public void add(ProtoBloomFilter pbf) {
		bitSet.or(pbf.create(config).bitSet);
		hamming = null;
		logValue = null;
	}

	/**
	 * Return true if this & other = this
	 * 
	 * @param other
	 *            the other bloom filter to match.
	 * @return true if they match.
	 */
	@Override
	public final boolean match(final BloomFilter other) {
		BitSet temp = BitSet.valueOf(this.bitSet.toByteArray());
		temp.and(other.bitSet);
		return temp.equals(this.bitSet);
	}

	public final boolean match(final ProtoBloomFilter other) {
		return match(other.create(config));
	}

	@Override
	public final boolean inverseMatch(final BloomFilter other) {
		return other.match(this);
	}

	public final boolean inverseMatch(final ProtoBloomFilter other) {
		return inverseMatch(other.create(config));
	}

	public final int distance(final BloomFilter other) {
		BitSet temp = BitSet.valueOf(this.bitSet.toByteArray());
		temp.xor(other.bitSet);
		return temp.cardinality();
	}

	public final int distance(final ProtoBloomFilter other) {
		return distance(other.create(config));
	}

	/**
	 * Get the hamming weight for this filter.
	 * 
	 * Ths is the number of bits that are on in the filter.
	 * 
	 * @return The hamming weight.
	 */
	public final int getHammingWeight() {
		if (hamming == null) {
			hamming = bitSet.cardinality();
		}
		return hamming;
	}

	public final double getLog() {
		if (logValue == null) {
			logValue = getApproximateLog(config.getNumberOfBits());
		}
		return logValue;
	}

	/**
	 * Get the approximate log for this filter. If the bloom filter is
	 * considered as an unsigned number what is the approximate base 2 log of
	 * that value. The depth argument indicates how many extra bits are to be
	 * considered in the log calculation. At least one bit must be considered.
	 * If there are no bits on then the log value is 0.
	 * 
	 * @see AbstractBloomFilter.getApproximateLog()
	 * @param depth
	 *            the number of bits to consider.
	 * @return the approximate log.
	 */
	public final double getApproximateLog(int depth) {
		int[] exp = getApproximateLogExponents(depth);
		/*
		 * this approximation is calculated using a derivation of
		 * http://en.wikipedia.org/wiki/Binary_logarithm#Algorithm
		 */
		// the mantissa is the highest bit that is turned on.
		if (exp[0] < 0) {
			// there are no bits so return 0
			return 0;
		}
		double result = exp[0];
		// now we move backwards from the highest bit until the requested
		// is achieved.
		double exp2;
		for (int i = 1; i < exp.length; i++) {
			if (exp[i] == -1) {
				return result;
			}
			exp2 = exp[i] - exp[0]; // should be negative
			result += Math.pow(2.0, exp2);
		}
		return result;
	}

	/**
	 * The mantissa of the log in in position position 0. The remainder are
	 * characteristic powers.
	 * 
	 * @param depth
	 * @return An array of depth integers that are the exponents.
	 */
	private final int[] getApproximateLogExponents(int depth) {
		int[] exp = new int[depth + 1];

		exp[0] = bitSet.length() - 1;
		if (exp[0] < 0) {
			return exp;
		}

		for (int i = 1; i < depth; i++) {
			exp[i] = bitSet.previousSetBit(exp[i - 1] - 1);
			if (exp[i] - exp[0] < -25) {
				exp[i] = -1;
			}
			if (exp[i] == -1) {
				return exp;
			}
		}
		return exp;
	}

	@Override
	public String toString() {
		return bitSet.toString();
	}

	@Override
	public int compareTo(BloomFilter o) {
		return Double.compare(this.getLog(), o.getLog());
	}

	public boolean isEqual(BloomFilter o) {
		return this.bitSet.equals(o.bitSet);
	}
}
