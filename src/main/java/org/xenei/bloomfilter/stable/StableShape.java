package org.xenei.bloomfilter.stable;

import org.apache.commons.collections4.bloomfilter.Shape;

public class StableShape {

    private Shape shape;
    /**
     * The value to set the cell when it is enabled. In the paper this is called
     * "Max". resetValue = (2^bitsPerCell)-1
     */
    public final int resetValue;
    public final Shape decrementShape;
    public final int expectedCardinality;
    public final double fps;
    /**
     * Definition 2 (Stable Point). The stable point is defined as the limit of the
     * expected fraction of 0s in an SBF when the number of iterations goes to
     * infinity. When this limit is reached, we call SBF stable
     */
    public final double stablePoint;
    public final byte bitsPerCell;
    public final byte cellsPerByte;

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Shape shape) {
        return new Builder(shape);
    }

    private StableShape(double fps, int m, int k, int p, int max) {
        this.shape = Shape.fromPMK(fps, m, k);
        this.resetValue = max;
        this.decrementShape = Shape.fromKM(p, m);
        this.fps = fps;
        int bits = Byte.SIZE;
        for (int i = 1; i < Byte.SIZE; i++) {
            if ((max >> i) == 0) {
                bits = i;
                break;
            }
        }
        this.bitsPerCell = (byte) bits;
        this.cellsPerByte = (byte) (Byte.SIZE / bitsPerCell);

        this.stablePoint = Math.pow(1.0 / (1 + (1.0 / (p * ((1.0 / k) - (1.0 / m))))), max);
        this.expectedCardinality = (int) Math.ceil((1.0 - stablePoint) * m);
    }

    @Override
    public String toString() {
        return String.format(
                "StableShape[k=%s m=%s fps=%s stable point=%s expected cardinality=%s decrement count=%s reset value=%s]",
                getNumberOfHashFunctions(), getNumberOfEntries(), fps, stablePoint, expectedCardinality,
                decrementShape.getNumberOfHashFunctions(), resetValue);
    }

    public Shape getShape() {
        return shape;
    }

    int getNumberOfHashFunctions() {
        return shape.getNumberOfHashFunctions();
    }

    int getNumberOfEntries() {
        return shape.getNumberOfBits();
    }

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

        public Builder setFps(double fps) {
            this.fps = fps;
            return this;
        }

        public Builder setK(int k) {
            this.k = k;
            return this;
        }

        public Builder setM(int m) {
            this.m = m;
            return this;
        }

        public Builder setP(int p) {
            this.p = p;
            return this;
        }

        public Builder setMax(int max) {
            this.max = max;
            return this;
        }

        public Builder setD(int d) {
            if (d > Byte.SIZE || d < 1) {
                throw new IllegalArgumentException("D must be in the range [1,8]");
            }
            max = (1 << d) - 1;
            return this;
        }

        private void checkSettings() {
            if (m < 1) {
                throw new IllegalArgumentException("M must be greater than 1");
            }
            if (k <= UNSET && fps <= UNSET) {
                throw new IllegalArgumentException("Either K or Fps must be greater than 0");
            }
            if (max <= UNSET || max > 0xFF) {
                throw new IllegalArgumentException("Max must be in the range [1,255]");
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

        public StableShape build() {
            checkSettings();
            return new StableShape(fps, m, k, p, max);
        }
    }
}