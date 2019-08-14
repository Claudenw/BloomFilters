package org.xenei.bloomfilter.collections;

import java.util.Collection;
import java.util.function.Function;

import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.ProtoBloomFilter;

public interface BloomCollection<T> extends Collection<T>, BloomFilterGated {

	public FilterConfig getConfig();

	public Function<T, ProtoBloomFilter> getFunc();

	/**
	 * Add an entry to the collection
	 * 
	 * @param proto the proto bloom filter.
	 * @param t     the data element
	 * @return true if this collection changed as a result of the call
	 */
	public boolean add(ProtoBloomFilter proto, T t);

	/**
	 * Returns true if this collection contains the object.
	 * 
	 * @param proto the proto bloom filter
	 * @param t     the data element
	 * @return true if this collection contains the object.
	 */
	public boolean contains(ProtoBloomFilter proto, T t);

	/**
	 * Remove the object from the collection.
	 * 
	 * @param proto the proto bloom filter
	 * @param t     the data element
	 * @return true if this collection changed as a result of the call
	 * @throws UnsupportedOperationException If the remove operation is not
	 *                                       supported by this collection
	 */
	public boolean remove(ProtoBloomFilter proto, T t);

	/**
	 * Get an iterator of candidates that match the proto bloom filter. This is a
	 * bloom filter match and may result in false positives.
	 * 
	 * @param pbf the bloom filter to match.
	 * @return an iterator over candidate data items.
	 */
	public ExtendedIterator<T> getCandidates(ProtoBloomFilter pbf);

	/**
	 * Get any exact matches for the proto bloom filter. This is an exact match with
	 * the bloom filter definition and may result in duplicates.
	 * 
	 * @param proto the proto bloom filter to match.
	 * @return an iterator over exact match data items.
	 */
	public ExtendedIterator<T> getExactMatches(ProtoBloomFilter proto);
	
	/**
	 * Get the collections statistics.
	 * @return
	 */
	public CollectionStats getStats();

}
