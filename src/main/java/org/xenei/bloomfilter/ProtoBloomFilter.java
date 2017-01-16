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
public class ProtoBloomFilter implements BloomFilterI<ProtoBloomFilter> {

	// the hamming value once we have calculated it.
	private long[] hashes;

	/**
	 * Constructor
	 */
	public ProtoBloomFilter(long[] hashes) {
		this.hashes = hashes;
	}

	/**
	 * Return true if this & other = this
	 * 
	 * @param other
	 *            the other bloom filter to match.
	 * @return true if they match.
	 */
	public final BloomFilter create(FilterConfig cfg) {
		BitSet set = new BitSet(cfg.getNumberOfBits());
		for (int i = 0; i < cfg.getNumberOfHashFunctions(); i++) {
			int j = Math.abs((int) ((hashes[0] + (i * hashes[1])) % cfg.getNumberOfBits()));
			set.set(j, true);
		}
		return new BloomFilter(cfg, set);
	}

	@Override
	public int hashCode() {
		return (int) ((hashes[0] ^ (hashes[0] >>> 32)) ^ (hashes[1] ^ hashes[1] >>> 32));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof ProtoBloomFilter) {
			ProtoBloomFilter other = (ProtoBloomFilter) o;
			return other.hashes[0] == hashes[0] && other.hashes[1] == hashes[1];
		}
		return false;
	}

	@Override
	public int compareTo(ProtoBloomFilter pbf) {
		int retval = Long.compare(hashes[0], pbf.hashes[0]);
		return (retval == 0) ? Long.compare(hashes[1], pbf.hashes[1]) : retval;
	}

	@Override
	public String toString() {
		return String.format("ProtoBloomFilter[ %s, %s]", hashes[0], hashes[1]);
	}

	@Override
	public boolean match(BloomFilter other) {
		return other.inverseMatch(this);
	}

	@Override
	public boolean inverseMatch(BloomFilter other) {
		return other.match(this);
	}

}
