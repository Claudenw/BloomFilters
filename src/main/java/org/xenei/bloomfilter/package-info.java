/**
 * A collection of bloom filter classes.
 * 
 * <b>Usage Pattern</b>
 * <ul>
 * <li>Use a BloomFilterBuilder to digest items and create a
 * ProtoBloomFilter.</li>
 * <li>Create a FilterConfig defining the number of items that will be added to
 * the filter and the probability of collisions.</li>
 * <li>Use the ProtoBloomFilter to create a BloomFilter with the shape defined
 * by the FilterConfig.</li>
 * </ul>
 * 
 * <b>Notes</b>
 * 
 * <ul>
 * <li>The ProtoBloomFilter can be used to generate multiple BloomFilters using
 * multiple FilterConfigs.</li>
 * <li>Creation of the ProtoBloomFilter is far more intensive than the creation
 * of the subsequent BloomFilter.</li>
 * </ul>
 * 
 */
package org.xenei.bloomfilter;