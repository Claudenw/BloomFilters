package org.xenei.bloomfilter.stable;

import org.apache.commons.collections4.bloomfilter.Shape;

public class StableShape implements CellShape {

    private Shape shape;
    /**
     * The value to set the cell when it is enabled. In the paper this is called
     * "Max". resetValue = (2^bitsPerCell)-1
     */
    private final int resetValue;
    /**
     * The shape used when decrementing the filter.
     */
    public final Shape decrementShape;
    /**
     * The cardinality expected when filter is stable.
     */
    public final int expectedCardinality;
    /**
     * The false positive rate when filter is stable.
     */
    public final double fps;
    /**
     * The stable point is defined as the limit of the
     * expected fraction of 0s in an SBF when the number of iterations goes to
     * infinity. When this limit is reached, we call SBF stable.
     */
    public final double stablePoint;
    /**
     * The number of bits per cell/entry.
     */
    private final byte bitsPerCell;

    /**
     * Constructs an empty builder.
     * @return an empty builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Constructs an builder with {@code k} and {@code m} based on the shape.
     * @param shape to base the stable shape on.
     * @return
     */
    public static Builder builder(Shape shape) {
        return new Builder(shape);
    }

    private StableShape(double fps, int m, int k, int p, int max) {
        this.shape = Shape.fromPMK(fps, m, k);
        this.resetValue = max;
        this.decrementShape = Shape.fromKM(p, m);
        this.fps = fps;
        int bits = Byte.SIZE;
        for (int i = 1; i < Integer.SIZE; i++) {
            if ((max >> i) == 0) {
                bits = i;
                break;
            }
        }
        this.bitsPerCell = (byte) bits;

        this.stablePoint = Math.pow(1.0 / (1 + (1.0 / (p * ((1.0 / k) - (1.0 / m))))), max);
        this.expectedCardinality = (int) Math.ceil((1.0 - stablePoint) * m);
        verifySettings();
    }
    
    
  /**
   * Test that the settings of the shape are reasonable.
   * @param shape
   */
  private void verifySettings() {
      if (resetValue() > maxValue() || resetValue() < 1) {
          throw new IllegalStateException("reset value must be in the range [1,255]");
      }
      if (Math.pow(2, bitsPerCell()) < resetValue()) {
          throw new IllegalStateException( String.format( "2^%s > %s", bitsPerCell(), resetValue()));
      }
  }

    @Override
    public String toString() {
        return String.format(
                "StableShape[k=%s m=%s fps=%s stable point=%s expected cardinality=%s decrement count=%s reset value=%s]",
                getNumberOfHashFunctions(), numberOfCells(), fps, stablePoint, expectedCardinality,
                decrementShape.getNumberOfHashFunctions(), resetValue);
    }

    /**
     * Gets the standard Bloom filter Shape for the stable Bloom filter.
     * @return The standard Bloom filter shape.
     */
    @Override
    public Shape getShape() {
        return shape;
    }

    /**
     * Gets the number of hash functions used to construct the filter.
     * This is also known as {@code k}.
     *
     * @return the number of hash functions used to construct the filter ({@code k}).
     */
    int getNumberOfHashFunctions() {
        return shape.getNumberOfHashFunctions();
    }

    @Override
    public byte bitsPerCell() {
        return bitsPerCell;
    }

    public int resetValue() {
        return resetValue;
    }

    /**
     * A builder class for the StableShape.
     *
     */
    public static class Builder {
        private static final int UNSET = 0;
        // false positive stable rate
        private double fps = UNSET;
        // number of hashes
        private int k = UNSET;
        // number of bits
        private int m = UNSET;;
        // number of cells to decrement
        private int p = UNSET;
        // value to reset to
        private int max = 2;

        private Builder() {
        }

        private Builder(Shape shape) {
            setK(shape.getNumberOfHashFunctions()).setM(shape.getNumberOfBits());
        }

        /**
         * Sets the expected false positive rate.  if not set will be calculated from {@code k}.
         * @param fps the expected false positive rate.
         * @return this  for chaining.
         */
        public Builder setFps(double fps) {
            this.fps = fps;
            return this;
        }

        /**
         * Sets the number of hashes for each Bloom filter. if not set will be calculated from {@code fps}.
         * @param k the number of hashes for each filter.
         * @return this for chaining.
         */
        public Builder setK(int k) {
            this.k = k;
            return this;
        }

        /**
         * Sets the number of bits for each Bloom filter.  Must be greater than 1.
         * @param m the number of bits for each Bloom filter.
         * @return this for chaining.
         */
        public Builder setM(int m) {
            this.m = m;
            return this;
        }

        /**
         * Sets the number of cells to decrement on each insertion.
         * @param p the number of cells to decrement on each insertion.
         * @return this for chaining.
         */
        public Builder setP(int p) {
            this.p = p;
            return this;
        }

        /**
         * Sets the value to set in each cell on insertion.  Must be in the range [1,255]
         * @param max the value to set eaach cell on insertion.
         * @return this for chaining.
         */
        public Builder setMax(int max) {
            this.max = max;
            return this;
        }

        /**
         * Sets the number of bits to be used for each insertion.  Setting this value will
         * reset max to be the maximum value that will fit in the specified number of bits.
         * @param d the number of bits to use.  Must be in the range [1,8].
         * @return this for chaining.
         */
        public Builder setD(int d) {
            if (d > Byte.SIZE || d < 1) {
                throw new IllegalArgumentException("D must be in the range [1,8]");
            }
            max = (1 << d) - 1;
            return this;
        }

        private void checkSettings() {
            if (m <= 1) {
                throw new IllegalArgumentException("M must be greater than 1");
            }
            if (k <= UNSET && fps <= UNSET) {
                throw new IllegalArgumentException("Either K or Fps must be greater than 0");
            }
            if (max <= UNSET) {
                throw new IllegalArgumentException("Max must be greater than 0");
            }
            if (k <= UNSET) {
                // log2(1/fps) by log rule
                k = (int) Math.ceil(Math.log(1 / fps) / Math.log(2));
            }
            if (fps <= UNSET) {
                fps = 1 / Math.pow(2, k);
            }
            if (p <= UNSET) {
                double oneOverK = 1.0 / k;
                double leftDenom = 1.0 / Math.pow(1 - Math.pow(fps, oneOverK), (1.0 / max)) - 1;
                double rightDenom = oneOverK - 1.0 / m;
                p = (int) Math.ceil(1.0 / (leftDenom * rightDenom));
                if (p > m) {
                    // adjustment for cases where K is "close to" M
                    p = (int) Math.ceil(1.0 / (leftDenom * oneOverK));
                }
            }
        }

        /**
         * Builds the StableShape.
         * @return a new StableShape.
         */
        public StableShape build() {
            checkSettings();
            return new StableShape(fps, m, k, p, max);
        }
    }
}