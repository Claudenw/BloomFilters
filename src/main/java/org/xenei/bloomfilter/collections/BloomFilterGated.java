package org.xenei.bloomfilter.collections;

import org.xenei.bloomfilter.BloomFilterImpl;
import org.xenei.bloomfilter.ProtoBloomFilter;

public interface BloomFilterGated {

	public BloomFilterImpl getGate();

	/**
	 * Return true if this bloom filter is full. A full bloom filter is one that has
	 * the number of items defined in the gate filter configuration in the list.
	 * 
	 * A full list is not prohibited or otherwise restricted from accepting more
	 * entries but will be less efficient during searches.
	 * 
	 * @return true if the list is full.
	 */
	public boolean isFull();

	/**
	 * Calculates the hamming distance from the gate filter to a filter.
	 * 
	 * @param f The filter to calculate distance to.
	 * @return the distance
	 */
	public int distance(BloomFilterImpl f);

	/**
	 * Calculates the hamming distance to a proto filter.
	 * 
	 * @param pf The proto filter to calculate distance to.
	 * @return the distance
	 */
	public int distance(ProtoBloomFilter pf);

	/**
	 * Returns true if the filter is found in this list.
	 * 
	 * @param filter The filter to look for.
	 * @return true if this list contains the filter.
	 */
	public boolean matches(BloomFilterImpl filter);

	/**
	 * Returns true if pbf is found in this list.
	 * 
	 * @param pbf the proto bloom filter to check.
	 * @return true if this list contains the filter.
	 */
	public boolean matches(ProtoBloomFilter pbf);

	/**
	 * returns true if this gate is found in the pbf.
	 * 
	 * @param pbf the proto bloom filter to check.
	 * @return true if the proto bloom filter contains the gate for this list.
	 */
	public boolean inverseMatch(ProtoBloomFilter pbf);

	/**
	 * returns true if this gate is found in the bf.
	 * 
	 * @param bf the bloom filter to check.
	 * @return true if the bloom filter contains the gate for this list.
	 */
	public boolean inverseMatch(BloomFilterImpl bf);

}
