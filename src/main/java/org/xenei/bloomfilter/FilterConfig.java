package org.xenei.bloomfilter;

import java.io.Serializable;

/**
 * Filter configuration class.
 *
 * This class contains the values for the filter configuration.
 *
 * @see <a href="http://hur.st/bloomfilter?n=3&p=1.0E-5">Bloom Filter calculator</a>
 *
 */
public class FilterConfig implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8857015449149940190L;
	private static final double LOG_OF_2 = Math.log(2.0);
	private static final double DENOMINATOR = Math.log(1.0 / (Math.pow(2.0, LOG_OF_2)));
	// number of items in the filter
	private int numberOfItems;
	// probability of false positives defined as 1 in x;
	private int probability;
	// number of bits in the filter;
	private int numberOfBits;
	// number of hash functions
	private int numberOfHashFunctions;
	
	/**
	 * A main method to generate and output the results of different constructor arguments.
	 * 
	 * Arguments:
	 * <ol>
	 * <li>The number of items to put in the bloom filter</li>
	 * <li>The probability of a collision expressed as X in 1/X</li>
	 * </ol>
	 * 
	 * Outputs the statistics of the filter configuration.
	 * 
	 * @param args the arguments
	 */
	public static void main(String[] args)
	{
		FilterConfig fc = new FilterConfig( Integer.parseInt(args[0]), Integer.parseInt(args[1]));
		System.out.println( String.format( "items: %s bits: %s bytes: %s functions: %s p: 1/%s (%s)", 
				fc.getNumberOfItems(), fc.getNumberOfBits(), fc.getNumberOfBytes(),
				fc.getNumberOfHashFunctions(), fc.getProbability(), (1.0/fc.getProbability())));
	}

	/**
	 * Create a filter configuration with the specified number of bits and
	 * probability.
	 * 
	 * @param numberOfItems
	 *            Number of items to be placed in the filter.
	 * @param probability
	 *            The probability of duplicates expressed as 1 in x.
	 */
	public FilterConfig(final int numberOfItems, final int probability) {
		this.numberOfItems = numberOfItems;
		this.probability = probability;
		final double dp = 1.0 / probability;
		final Double dm = Math.ceil((numberOfItems * Math.log(dp)) / DENOMINATOR);
		if (dm > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Resulting filter has more than " + Integer.MAX_VALUE + " bits");
		}
		this.numberOfBits = dm.intValue();
		final Long lk = Math.round((LOG_OF_2 * numberOfBits) / numberOfItems);
		if (lk > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(
					"Resulting filter has more than " + Integer.MAX_VALUE + " hash functions");
		}
		numberOfHashFunctions = lk.intValue();
	}

	/**
	 * Get the number of items that are expected in the filter. AKA: n
	 * 
	 * @return the number of items.
	 */
	public int getNumberOfItems() {
		return numberOfItems;
	}

	/**
	 * The probability of a false positive (collision) expressed as 1/x. AKA:
	 * 1/p
	 * 
	 * @return the x in 1/x.
	 */
	public int getProbability() {
		return probability;
	}

	/**
	 * The number of bits in the bloom filter. AKA: m
	 * 
	 * @return the number of bits in the bloom filter.
	 */
	public int getNumberOfBits() {
		return numberOfBits;
	}

	/**
	 * The number of hash functions used to construct the filter. AKA: k
	 * 
	 * @return the number of hash functions used to construct the filter.
	 */
	public int getNumberOfHashFunctions() {
		return numberOfHashFunctions;
	}

	/**
	 * The number of bytes in the bloom filter.
	 * 
	 * @return the number of bytes in the bloom filter.
	 */
	public int getNumberOfBytes() {
		return Double.valueOf(Math.ceil(numberOfBits / 8.0)).intValue();
	}

}