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

import java.nio.LongBuffer;
import java.util.BitSet;
import java.util.Objects;

import org.xenei.bloomfilter.hasher.Hasher;
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
/**
 * The interface that defines a bloom filter.
 *
 */
public interface BloomFilter {

    /**
     * Gets the LongBuffer representation of this filter.
     * @return the LongBuffer representation of this filter.
     */
    LongBuffer getBits();

    /**
     * Gets the shape of this filter.
     * @return The shape of this filter.
     */
    Shape getShape();

    /**
     * Merge the other Bloom filter into this one.
     * @param other the other Bloom filter.
     */
    void merge( BloomFilter other );

    /**
     * Gets the cardinality of this Bloom filter.
     * @return the cardinality (number of enabled bits) in this filter.
     */
    default int cardinality() {
        return BitSet.valueOf( getBits() ).cardinality();
    }

    /**
     * Performs a logical "AND" with the other Bloom filter and returns the
     * cardinality of the result.
     * @param other the other Bloom filter.
     * @return the cardinality of the result of ( this AND other ).
     */
    default int andCardinality( BloomFilter other ) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        LongBuffer mine = getBits();
        LongBuffer theirs = other.getBits();
        LongBuffer remainder = null;
        long[] result = null;
        if ( mine.limit() > theirs.limit()) {
            result = new long[ mine.limit() ];
            remainder = mine;
        } else {
            result = new long[ theirs.limit() ];
            remainder = theirs;

        }
        int limit = Integer.min(mine.limit(), theirs.limit());
        for (int i = 0;i<limit;i++)
        {
            result[i] = mine.get(i) & theirs.get(i);
        }
        for (int i=limit;i<result.length-1;i++)
        {
            result[i] = remainder.get(i);
        }
        return BitSet.valueOf( result ).cardinality();
    }

    /**
     * Performs a logical "OR" with the other Bloom filter and returns the
     * cardinality of the result.
     * @param other the other Bloom filter.
     * @return the cardinality of the result of ( this OR other ).
     */
    default int orCardinality( BloomFilter other ) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        LongBuffer mine = getBits();
        LongBuffer theirs = other.getBits();
        LongBuffer remainder = null;
        long[] result = null;
        if ( mine.limit() > theirs.limit()) {
            result = new long[ mine.limit() ];
            remainder = mine;
        } else {
            result = new long[ theirs.limit() ];
            remainder = theirs;

        }
        int limit = Integer.min(mine.limit(), theirs.limit());
        for (int i = 0;i<limit;i++)
        {
            result[i] = mine.get(i) | theirs.get(i);
        }
        for (int i=limit;i<result.length-1;i++)
        {
            result[i] = remainder.get(i);
        }
        return BitSet.valueOf( result ).cardinality();
    }

    /**
     * Performs a logical "XOR" with the other Bloom filter and returns the
     * cardinality of the result.
     * @param other the other Bloom filter.
     * @return the cardinality of the result of ( this XOR other ).
     */
    default int xorCardinality( BloomFilter other ) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        LongBuffer mine = getBits();
        LongBuffer theirs = other.getBits();
        LongBuffer remainder = null;
        long[] result = null;
        if ( mine.limit() > theirs.limit()) {
            result = new long[ mine.limit() ];
            remainder = mine;
        } else {
            result = new long[ theirs.limit() ];
            remainder = theirs;

        }
        int limit = Integer.min(mine.limit(), theirs.limit());
        for (int i = 0;i<limit;i++)
        {
            result[i] = mine.get(i) ^ theirs.get(i);
        }
        for (int i=limit;i<result.length-1;i++)
        {
            result[i] = remainder.get(i);
        }
        return BitSet.valueOf( result ).cardinality();
    }

    /**
     * Performs a match check.  Effectively this AND other == this.
     * @param other the Other Bloom filter.
     * @return true if this filter matches the other.
     */
    default boolean match( BloomFilter other ) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        return cardinality() == andCardinality( other );
    }

    /**
     * Gets the Hamming value of this Bloom filter.
     * @return the hamming value.
     */
    default int hammingValue() {
        return cardinality();
    }

    /**
     * Gets the Hamming distance to the other Bloom filter.
     * @param other the Other bloom filter.
     * @return the Hamming distance.
     */
    default int hammingDistance( BloomFilter other ) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        return xorCardinality( other );
    }

    /**
     * Gets the Jaccard similarity wih the other Bloom filter.
     * @param other the Other bloom filter.
     * @return the Jaccard similarity.
     */
    default double jaccardSimilarity( BloomFilter other ) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        int orCard = orCardinality( other );
        if (orCard == 0)
        {
            return 0;
        }
        return hammingDistance( other ) / (double) orCard;
    }

    /**
     * Gets the jaccard distance to the other Bloom filter.
     * @param other the Other Bloom filter.
     * @return the jaccard distance.
     */
    default double jaccardDistance( BloomFilter other ) {
        return 1.0 - jaccardSimilarity( other );
    }

    /**
     * Gets the Cosine similarity wih the other Bloom filter.
     * @param other the Other bloom filter.
     * @return the Cosine similarity.
     */
    default double cosineSimilarity( BloomFilter other ) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        return andCardinality( other ) / (Math.sqrt( cardinality() ) * Math.sqrt( other.cardinality()));
    }

    /**
     * Gets the jaccard distance to the other Bloom filter.
     * @param other the Other Bloom filter.
     * @return the jaccard distance.
     */
    default double cosineDistance( BloomFilter other ) {
        return 1.0 - cosineSimilarity( other );
    }

    /**
     * Estimates the number of items in the Bloom filter based on the shape and the
     * number of bits that are enabled.
     * @return and estimate of the number of items that were placed in the Bloom filter.
     */
    default long estimateSize() {
        double estimate = -(getShape().getNumberOfBits() * 1.0 / getShape().getNumberOfHashFunctions()) *
            Math.log(1.0 - (hammingValue() * 1.0 / getShape().getNumberOfBits()));
        return Math.round( estimate );
    }

    /**
     * Estimates the number of items in the union of the sets of items that created the bloom filters.
     *
     * @param other the other Bloom filter.
     * @return an estimate of the size of the union between the two filters.
     */
    default long estimateUnionSize(BloomFilter other) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        double estimate = -(getShape().getNumberOfBits() * 1.0 / getShape().getNumberOfHashFunctions()) *
            Math.log(1 - orCardinality(other) * 1.0 / getShape().getNumberOfBits());
        return Math.round( estimate );
    }

    /**
     * Estimates the number of items in the intersection of the sets of items that created the bloom filters.
     *
     * @param other the other Bloom filter.
     * @return an estimate of the size of the intersection between the two filters.
     */
    default long estimateIntersectionSize(BloomFilter other) {
        if (!getShape().equals( other.getShape() )) {
            throw new IllegalArgumentException( "Other does not have same shape");
        }
        // do subtraction early to avoid Long overflow.
        return estimateSize() - estimateUnionSize(other) + other.estimateSize();
    }

    /**
     * Determines if the bloom filter is "full".
     * Full is definded as haveing no unset bits.
     * @param filter the filter to check.
     * @return true if the filter is full.
     */
    default boolean isFull() {
        return hammingValue() == getShape().getNumberOfBits();
    }

    /**
     * The definition of a filter configuration. A simple Bloom filter configuration
     * implementation that derives the values from the number of items and the probability of
     * collision.
     *
     * <p> This interface defines the values for the filter configuration and is used to
     * convert a ProtoBloomFilter into a BloomFilter. </p>
     *
     * <p> This class contains the values for the filter configuration and is used to convert
     * a ProtoBloomFilter into a BloomFilter. </p>
     *
     * <h2>Interrelatedness of values</h2>
     *
     *  <dl>
     *  <dt>Number of Items (AKA: {@code n})</dt>
     *  <dd>{@code n = ceil(m / (-k / log(1 - exp(log(p) / k))))}</dd>
     *  <dt>Probability of Collision (AKA: {@code p})</dt>
     *  <dd>{@code p =  (1 - exp(-kn/m))^k}</dd>
     *  <dt>Number of Bits (AKA: {@code m})</dt>
     *  <dd>{@code m = ceil((n * log(p)) / log(1 / pow(2, log(2))))}</dd>
     *  <dt>Number of Functions (AKA: {@code k})</dt>
     *  <dd>{@code k = round((m / n) * log(2))}</dd>
     *  </dl>
     * @see <a href="http://hur.st/bloomfilter?n=3&p=1.0E-5">Bloom Filter calculator</a>
     * @see <a href="https://en.wikipedia.org/wiki/Bloom_filter">Bloom filter [Wikipedia]</a>
     */
    public class Shape {

        /**
         * The natural logarithm of 2. Used in several calculations. approx 0.693147180
         */
        private static final double LOG_OF_2 = Math.log(2.0);

        /**
         * 1 / 2^log(2) approx −0.090619058. Used in calculating the number of bits.
         */
        private static final double DENOMINATOR = Math.log(1.0 / (Math.pow(2.0, LOG_OF_2)));
        /**
         * number of items in the filter. (AKA: {@code n})
         */
        private final int numberOfItems;
        /**
         * probability of false positives. (AKA: {@code p})
         */
        private final double probability;
        /**
         * number of bits in the filter.  (AKA: {@code m})
         */
        private final int numberOfBits;
        /**
         * number of hash functions. (AKA: {@code k})
         */
        private final int numberOfHashFunctions;

        /**
         * The hash code for this filter.
         */
        private final int hashCode;

        /**
         * The name of the hasher function.
         */
        private final String hasherName;

        /**
         * Create a filter configuration with the specified number of items and probability.
         * <p>
         * The actual probability will be approximately equal to the desired probability but will
         * be dependent upon the caluclated bloom filter size and function count.
         * </p>
         * @param hasher The Hasher function to use for this shape.
         * @param numberOfItems Number of items to be placed in the filter.
         * @param probability The desired probability of duplicates. Must be in the range (0.0,1.0).
         */
        public Shape(Hasher hasher, final int numberOfItems, final double probability) {
            if (hasher == null) {
                throw new IllegalArgumentException("Hasher may not be null");
            }
            if (numberOfItems < 1) {
                throw new IllegalArgumentException("Number of Items must be greater than 0");
            }
            if (probability <= 0.0) {
                throw new IllegalArgumentException("Probability must be greater than 0.0");
            }
            if (probability >= 1.0) {
                throw new IllegalArgumentException("Probability must be less than 1.0");
            }
            this.hasherName = hasher.getName();
            this.numberOfItems = numberOfItems;
            /*
             * number of bits is called "m" in most mathematical statement describing bloom
             * filters so we use it here.
             */
            final double m = Math.ceil(numberOfItems * Math.log(probability) / DENOMINATOR);
            if (m > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Resulting filter has more than " + Integer.MAX_VALUE + " bits");
            }
            this.numberOfBits = (int)m;
            numberOfHashFunctions = calculateNumberOfHashFunctions( numberOfItems, numberOfBits );
            this.probability = calculateProbability( numberOfItems, numberOfBits, numberOfHashFunctions );
            hashCode = Objects.hash(hasherName, numberOfBits, numberOfHashFunctions, numberOfItems, probability);
        }

        /**
         * Create a filter configuration with the specified number of items and probability.
         *
         * @param hasher The Hasher function to use for this shape.
         * @param numberOfItems Number of items to be placed in the filter.
         * @param numberOfBits The number of bits in the filter.
         */
        public Shape(final Hasher hasher, final int numberOfItems, final int numberOfBits) {
            if (hasher == null) {
                throw new IllegalArgumentException("Hasher may not be null");
            }
            if (numberOfItems < 1) {
                throw new IllegalArgumentException("Number of Items must be greater than 0");
            }
            if (numberOfBits < 8) {
                throw new IllegalArgumentException("Number of Bits must be greater than or equal to 8");
            }
            this.hasherName = hasher.getName();
            this.numberOfItems = numberOfItems;
            this.numberOfBits = numberOfBits;
            this.numberOfHashFunctions = calculateNumberOfHashFunctions(numberOfItems, numberOfBits);
            this.probability = calculateProbability( numberOfItems, numberOfBits, numberOfHashFunctions );
            hashCode = Objects.hash(hasherName, numberOfBits, numberOfHashFunctions, numberOfItems, probability);
        }

        /**
         * Create a filter configuration with the specified number of items and probability.
         *
         * @param hasher The Hasher function to use for this shape.
         * @param numberOfItems Number of items to be placed in the filter.
         * @param numberOfBits The number of bits in the filter.
         * @param numberOfHashFunctions The number of hash functions in the filter.
         */
        public Shape(final Hasher hasher, final int numberOfItems, final int numberOfBits, final int numberOfHashFunctions) {
            if (hasher == null) {
                throw new IllegalArgumentException("Hasher may not be null");
            }
            if (numberOfItems < 1) {
                throw new IllegalArgumentException("Number of Items must be greater than 0");
            }
            if (numberOfBits < 8) {
                throw new IllegalArgumentException("Number of Bits must be greater than or equal to 8");
            }
            if (numberOfHashFunctions < 1)
            {
                throw new IllegalArgumentException("Number of Hash Functions must be greater than or equal to 8");
            }
            this.hasherName = hasher.getName();
            this.numberOfItems = numberOfItems;
            this.numberOfBits = numberOfBits;
            this.numberOfHashFunctions = numberOfHashFunctions;
            this.probability = calculateProbability( numberOfItems, numberOfBits, numberOfHashFunctions );
            hashCode = Objects.hash(hasherName, numberOfBits, numberOfHashFunctions, numberOfItems, probability);
        }

        /**
         * Create a filter configuration with the specified number of items and probability.
         *
         * @param hasher The Hasher function to use for this shape.
         * @param probability The probability of duplicates. Must be in the range (0.0,1.0).
         * @param numberOfBits The number of bits in the filter.
         * @param numberOfHashFunctions The number of hash functions in the filter.
         */
        public Shape(final Hasher hasher, final double probability, final int numberOfBits, final int numberOfHashFunctions) {
            if (hasher == null) {
                throw new IllegalArgumentException("Hasher may not be null");
            }
            if (probability <= 0.0) {
                throw new IllegalArgumentException("Probability must be greater than 0.0");
            }
            if (probability >= 1.0) {
                throw new IllegalArgumentException("Probability must be less than 1.0");
            }
            if (numberOfBits < 8) {
                throw new IllegalArgumentException("Number of bits must be greater than or equal to 8");
            }
            if (numberOfHashFunctions < 1) {
                throw new IllegalArgumentException("Number of hash functions must be greater than or equal to 8");
            }
            this.hasherName = hasher.getName();
            this.numberOfBits = numberOfBits;
            this.numberOfHashFunctions = numberOfHashFunctions;


            // n = ceil(m / (-k / log(1 - exp(log(p) / k))))
            double n = Math.ceil(numberOfBits /
                (-numberOfHashFunctions / Math.log(1 - Math.exp(Math.log(probability) / numberOfHashFunctions))));

            // log of probability is always < 0
            // number of hash functions is >= 1
            // e^x where x < 0 = [0,1)
            // log 1-e^x = [log1, log0) = <0 with an effective lower limit of -53
            // numberOfBits/ (-numberOfHashFunctions / [-53,0) ) >0
            // ceil( >0 ) >= 1
            // so we can not produce a negative value thus we don't chack for it.
            //
            // similarly we can not produce a number greater than numberOfBits so we
            // do not have to check for Integer.MAX_VALUE either.
            this.numberOfItems = (int) n;
            this.probability = calculateProbability( numberOfItems, numberOfBits, numberOfHashFunctions );
            hashCode = Objects.hash(hasherName, numberOfBits, numberOfHashFunctions, numberOfItems, probability);
        }

        /**
         * Verify that the other shape has the same hasher as this shape.
         * @param other the other shape.
         * @throws IllegalArgumentException if the hasher function names are not the same.
         */
        public final void verifyHasher( Shape other ) {
            if ( !hasherName.equals( other.hasherName))
            {
               throw new IllegalArgumentException( String.format("hasher name %s does not equal hasher name %s",
                       hasherName, other.hasherName));
            }
        }

        /**
         * Calculates the number of hash functions given numberOfItems and  numberofBits.
         * This is a method so that the calculation is consistent across all constructors.
         *
         * @param numberOfItems the number of items in the filter.
         * @param numberOfBits the number of bits in the filter.
         * @return the optimal number of hash functions.
         */
        private final int calculateNumberOfHashFunctions(int numberOfItems, int numberOfBits)
        {
            /*
             * k = round((m / n) * log(2)) We change order so that we use real math rather
             * than integer math.
             */
            long k = Math.round(LOG_OF_2 * numberOfBits / numberOfItems);
            if (k < 1) {
                throw new IllegalArgumentException(
                    String.format("Filter to small: Calculated number of hash functions (%s) was less than 1", k));
            }
            /*
             * normally we would check that numberofHashFunctions <= Integer.MAX_VALUE but since numberOfBits
             * is at most Integer.MAX_VALUE the numerator of numberofHashFunctions is log(2) * Integer.MAX_VALUE
             * = 646456992.9449 the value of k can not be above Integer.MAX_VALUE.
             */
            return (int) k;
        }

        /**
         * Calculates the probability given numberOfItems, numberofBits and numberOfHashFunctions.
         * This is a method so that the calculation is consistent across all constructors.
         *
         * @param numberOfItems the number of items in the filter.
         * @param numberOfBits the number of bits in the filter.
         * @param numberOfHashFunctions the number of hash functions used to create the filter.
         * @return the probability of collision.
         */
        private final double calculateProbability(int numberOfItems, int numberOfBits, int numberOfHashFunctions) {
            // (1 - exp(-kn/m))^k
            double p = Math.pow(1.0 - Math.exp(-1.0 * numberOfHashFunctions * numberOfItems / numberOfBits),
                numberOfHashFunctions);
            /*
             * We do not need to check for p < = since we only allow positive values for parameters
             * and the closest we can come to exp(-kn/m) == 1 is exp(-1/Integer.MAX_INT) approx 0.9999999995343387
             * so Math.pow( x, y ) will always be 0<x<1 and y>0
             */
            if (p >= 1.0) {
                throw new IllegalArgumentException(
                    String.format("Calculated probability (%s) is greater than or equal to 1.0", p));
            }
            return p;
        }

        /**
         * Gets the number of items that are expected in the filter. AKA: {@code n}
         *
         * @return the number of items.
         */
        public int getNumberOfItems() {
            return numberOfItems;
        }

        /**
         * Gets the probability of a false positive (collision). AKA: {@code p}
         *
         * @return the probability of a false positive.
         */
        public double getProbability() {
            return probability;
        }

        /**
         * Gets the number of bits in the Bloom filter. AKA: {@code m}
         *
         * @return the number of bits in the Bloom filter.
         */
        public int getNumberOfBits() {
            return numberOfBits;
        }

        /**
         * Gets the number of hash functions used to construct the filter. AKA: {@code k}
         *
         * @return the number of hash functions used to construct the filter.
         */
        public int getNumberOfHashFunctions() {
            return numberOfHashFunctions;
        }

        /**
         * Gets the number of bytes in the Bloom filter.
         *
         * @return the number of bytes in the Bloom filter.
         */
        public int getNumberOfBytes() {
            return Double.valueOf(Math.ceil(numberOfBits / 8.0)).intValue();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Shape) {
                Shape other = (Shape) o;
                return
                    other.getHasherName().equals( getHasherName() ) &&
                    other.getNumberOfBits() == getNumberOfBits() &&
                    other.getNumberOfHashFunctions() == getNumberOfHashFunctions() &&
                    other.getNumberOfItems() == getNumberOfItems() && other.getProbability() == getProbability();
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        public String getHasherName() {
            return hasherName;
        }
    }

}
