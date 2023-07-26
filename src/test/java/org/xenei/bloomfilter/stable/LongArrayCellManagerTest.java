package org.xenei.bloomfilter.stable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public class LongArrayCellManagerTest {

    private LongArrayCellManager underTest;
    private int evenReset = 2; // 2 bits
    private int oddReset = 4; // 3 bits
    private Shape testShape = Shape.fromNM(10, Long.SIZE); // Shape.fromNP(Long.SIZE, 1.0 / 5);

    @Test
    public void maskTest() {
        CellShape shape = CellShape.fromMaxValue(testShape, evenReset);
        underTest = new LongArrayCellManager(shape);
        long mask = 0x3L;
        for (int i = 0; i < underTest.cellsPerBlock; i++) {
            long expected = mask << i * shape.getBitsPerCell();
            int block2 = i + underTest.cellsPerBlock;
            assertEquals(expected, underTest.getMask(i), "Failed at cell " + i);
            assertEquals(expected, underTest.getMask(block2), "Failed at cell " + block2);
        }

        shape = CellShape.fromMaxValue(testShape, oddReset);
        underTest = new LongArrayCellManager(shape);
        mask = 0x7L;
        for (int i = 0; i < underTest.cellsPerBlock; i++) {
            long expected = mask << i * shape.getBitsPerCell();
            int block2 = i + underTest.cellsPerBlock;
            assertEquals(expected, underTest.getMask(i), "Failed at cell " + i);
            assertEquals(expected, underTest.getMask(block2), "Failed at cell " + block2);
        }

        shape = CellShape.fromMaxValue(testShape, evenReset);
        underTest = new LongArrayCellManager(shape);
        for (int i = 0; i < underTest.cellsPerBlock; i++) {
            int block2 = i + underTest.cellsPerBlock;
            for (int value = 0; value < (int) shape.cellMask(); value++) {
                long expected = ((long) value) << i * shape.getBitsPerCell();
                assertEquals(expected, underTest.getMask(i, value));
                assertEquals(expected, underTest.getMask(block2, value));
            }
        }
    }

    @Test
    public void constructorTest() {
        CellShape shape = CellShape.fromMaxValue(testShape, evenReset);
        underTest = new LongArrayCellManager(shape);
        assertEquals(32, underTest.cellsPerBlock);
        assertTrue(underTest.isValid());
        assertEquals(2, underTest.buffer.length);

        shape = CellShape.fromMaxValue(testShape, oddReset);
        underTest = new LongArrayCellManager(shape);
        assertEquals(21, underTest.cellsPerBlock);
        assertTrue(underTest.isValid());
        assertEquals(4, underTest.buffer.length);
    }

    @Test
    public void clearTest() {
        CellShape shape = CellShape.fromMaxValue(testShape, evenReset);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[0] = (byte) 0xCC;
        underTest.buffer[1] = (byte) 0xFF;
        underTest.clear();
        assertThat(underTest.buffer).containsOnly(0);

        shape = CellShape.fromMaxValue(testShape, oddReset);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[0] = (byte) 0xCC;
        underTest.buffer[1] = (byte) 0xFF;
        underTest.clear();
        assertThat(underTest.buffer).containsOnly(0);
    }

    @Test
    public void decrementEvenTest() {
        // decrement cell 1 and verify that values on either side to not change.
        CellShape shape = CellShape.fromMaxValue(testShape, evenReset);
        underTest = new LongArrayCellManager(shape);
        underTest.cardinality = 34;
        underTest.buffer[0] = 0xffff_ffff_ffff_ffffL;
        underTest.buffer[1] = 5;
        assertEquals(3, underTest.get(1));
        assertTrue(underTest.isValid());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_fffbL, underTest.buffer[0]);
        assertEquals(2, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(34, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_fff7L, underTest.buffer[0]);
        assertEquals(1, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(34, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0, underTest.get(1));
        assertEquals(0xffff_ffff_ffff_fff3L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(33, underTest.cardinality());

        // show decrement past 0 sets invalid.
        assertFalse(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_fff3L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertFalse(underTest.isValid());
    }

    @Test
    public void incrementEvenTest() {
        // decrement cell 1 and verify that values on either side to not change.
        CellShape shape = CellShape.fromBitsPerCell(testShape, 2);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[1] = 5;
        underTest.cardinality = 2;
        assertEquals(0, underTest.get(1));
        assertTrue(underTest.isValid());

        assertTrue(underTest.increment(1, 1));
        assertEquals(0x04L, underTest.buffer[0]);
        assertEquals(1, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        assertTrue(underTest.increment(1, 1));
        assertEquals(0x8L, underTest.buffer[0]);
        assertEquals(2, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        assertTrue(underTest.increment(1, 1));
        assertEquals(3, underTest.get(1));
        assertEquals(0xcL, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        // show increment past limit sets invalid.
        assertFalse(underTest.increment(1, 1));
        assertEquals(0xcL, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertFalse(underTest.isValid());
    }

    @Test
    public void decrementOddTest() {
        CellShape shape = CellShape.fromBitsPerCell(testShape, 3);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[0] = 0xffff_ffff_ffff_ffffL;
        underTest.buffer[1] = 5;
        underTest.cardinality = 23;
        assertEquals(7, underTest.get(1));
        assertTrue(underTest.isValid());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_fff7L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(23, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_ffefL, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(23, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_ffe7L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(23, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_ffdfL, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(23, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_ffd7L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(23, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_ffcfL, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(23, underTest.cardinality());

        assertTrue(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_ffc7L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(22, underTest.cardinality());

        // show decrement past 0 sets invalid
        assertFalse(underTest.decrement(1, 1));
        assertEquals(0xffff_ffff_ffff_ffc7L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertFalse(underTest.isValid());
    }

    @Test
    public void incrementOddTest() {
        // decrement cell 1 and verify that values on either side to not change.
        CellShape shape = CellShape.fromMaxValue(testShape, oddReset);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[1] = 5;
        underTest.cardinality = 2;
        assertEquals(0, underTest.get(1));
        assertTrue(underTest.isValid());

        assertTrue(underTest.increment(1, 1));
        assertEquals(0x08L, underTest.buffer[0]);
        assertEquals(1, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        assertTrue(underTest.increment(1, 1));
        assertEquals(0x10L, underTest.buffer[0]);
        assertEquals(2, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        assertTrue(underTest.increment(1, 1));
        assertEquals(3, underTest.get(1));
        assertEquals(0x18L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        assertTrue(underTest.increment(1, 1));
        assertEquals(4, underTest.get(1));
        assertEquals(0x20L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        // show increment past limit sets invalid.
        assertFalse(underTest.increment(1, 1));
        assertEquals(0x20L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertFalse(underTest.isValid());
    }

    @Test
    public void safeDecrementTest() {
        // decrement cell 1 and verify that values on either side to not change.
        CellShape shape = CellShape.fromMaxValue(testShape, evenReset);
        underTest = new LongArrayCellManager(shape);
        underTest.cardinality = 34;
        underTest.buffer[0] = 0xffff_ffff_ffff_ffffL;
        underTest.buffer[1] = 5;
        assertEquals(3, underTest.get(1));
        assertTrue(underTest.isValid());

        underTest.safeDecrement(1, 1);
        assertEquals(0xffff_ffff_ffff_fffbL, underTest.buffer[0]);
        assertEquals(2, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(34, underTest.cardinality());

        underTest.safeDecrement(1, 3);
        assertEquals(0xffff_ffff_ffff_fff3L, underTest.buffer[0]);
        assertEquals(0, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(33, underTest.cardinality());

        underTest.safeDecrement(1, 1);
        assertEquals(0, underTest.get(1));
        assertEquals(0xffff_ffff_ffff_fff3L, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(33, underTest.cardinality());
    }

    @Test
    public void safeIincrementTest() {
        // decrement cell 1 and verify that values on either side to not change.
        CellShape shape = CellShape.fromBitsPerCell(testShape, 2);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[1] = 5;
        underTest.cardinality = 2;
        assertEquals(0, underTest.get(1));
        assertTrue(underTest.isValid());

        underTest.safeIncrement(1, 1);
        assertEquals(0x04L, underTest.buffer[0]);
        assertEquals(1, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        underTest.safeIncrement(1, 3);
        assertEquals(0xcL, underTest.buffer[0]);
        assertEquals(3, underTest.get(1));
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());

        underTest.safeIncrement(1, 1);
        assertEquals(3, underTest.get(1));
        assertEquals(0xcL, underTest.buffer[0]);
        assertEquals(5, underTest.buffer[1]);
        assertTrue(underTest.isValid());
        assertEquals(3, underTest.cardinality());
    }
    @Test
    public void getTest() {
        CellShape shape = CellShape.fromMaxValue(testShape, oddReset);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[0] = 0xFAC688;
        for (int i = 0; i < oddReset; i++) {
            assertEquals(i, underTest.get(i));
        }
    }

    @Test
    public void isSetTest() {
        CellShape shape = CellShape.fromMaxValue(testShape, oddReset);
        underTest = new LongArrayCellManager(shape);
        underTest.buffer[0] = 0xC5;
        assertTrue(underTest.isSet(0));
        assertFalse(underTest.isSet(1));
        assertTrue(underTest.isSet(2));
        assertFalse(underTest.isSet(3));
    }

    @Test
    public void setTest() {
        CellShape shape = CellShape.fromMaxValue(testShape, 7);
        underTest = new LongArrayCellManager(shape);
        for (int i = 0; i < 8; i++) {
            assertTrue(underTest.set(i, i), "failed at " + i);
            assertTrue(underTest.isValid());
        }
        assertEquals(0xFAC688L, underTest.buffer[0]);

        // test setting to large a number
        assertFalse(underTest.set(0, 9));
        assertFalse(underTest.isValid());
        assertEquals(0xFAC688L, underTest.buffer[0]);

        // reset the buffer
        underTest.buffer[0] = 0xFAC688L;
        underTest.invalid = false;
        // test setting a negative a number
        assertFalse(underTest.set(1, -1));
        assertFalse(underTest.isValid());
        assertEquals(0xFAC688L, underTest.buffer[0]);
    }
}
