package org.xenei.bloomfilter.stable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public class StableShapeTest {

    StableShape underTest;
    Shape testShape = Shape.fromNP(5, 1.0 / 5);

    @Test
    public void numberOfCellsDecrementedTest() {
        underTest = StableShape.builder(testShape).build();
        assertEquals(6, underTest.decrementShape.getNumberOfHashFunctions());
    }

    @Test
    public void resetValueTest() {
        underTest = StableShape.builder(testShape).build();
        assertEquals(2, underTest.resetValue());
        underTest = StableShape.builder(testShape).setMax(128).build();
        assertEquals(128, underTest.resetValue());

        underTest = StableShape.builder(testShape).setMax(250).build();
        assertEquals(250, underTest.resetValue());

        assertThrows(IllegalArgumentException.class, () -> StableShape.builder(testShape).setMax(300).build());
        assertThrows(IllegalArgumentException.class, () -> StableShape.builder(testShape).setMax(0).build());
    }

    @Test
    public void bitsPerCellTest() {
        assertEquals(2, StableShape.builder(testShape).build().bitsPerCell());
        assertEquals(2, StableShape.builder(testShape).setMax(3).build().bitsPerCell());
        assertEquals(3, StableShape.builder(testShape).setMax(4).build().bitsPerCell());
        assertEquals(3, StableShape.builder(testShape).setMax(5).build().bitsPerCell());
        assertEquals(3, StableShape.builder(testShape).setMax(6).build().bitsPerCell());
        assertEquals(3, StableShape.builder(testShape).setMax(7).build().bitsPerCell());
        assertEquals(4, StableShape.builder(testShape).setMax(8).build().bitsPerCell());
        assertEquals(8, StableShape.builder(testShape).setMax(129).build().bitsPerCell());
        assertEquals(8, StableShape.builder(testShape).setMax(255).build().bitsPerCell());
    }

    @Test
    public void cellsPerByteTest() {
        assertEquals(4, StableShape.builder(testShape).build().cellsPerByte());
        assertEquals(4, StableShape.builder(testShape).setMax(3).build().cellsPerByte());
        assertEquals(2, StableShape.builder(testShape).setMax(4).build().cellsPerByte());
        assertEquals(2, StableShape.builder(testShape).setMax(5).build().cellsPerByte());
        assertEquals(2, StableShape.builder(testShape).setMax(6).build().cellsPerByte());
        assertEquals(2, StableShape.builder(testShape).setMax(7).build().cellsPerByte());
        assertEquals(2, StableShape.builder(testShape).setMax(8).build().cellsPerByte());
        assertEquals(1, StableShape.builder(testShape).setMax(16).build().cellsPerByte());
        assertEquals(1, StableShape.builder(testShape).setMax(129).build().cellsPerByte());
        assertEquals(1, StableShape.builder(testShape).setMax(255).build().cellsPerByte());
    }

    @Test
    public void expectedCardinalityTest() {
        assertEquals(9, StableShape.builder(testShape).build().expectedCardinality);
    }

    @Test
    public void fpsTest() {
        StableShape shape = StableShape.builder(testShape).build();
        assertEquals(0.25, shape.fps, 0.001);
    }

    @Test
    public void numberOfCellsTest() {
        assertEquals(17, StableShape.builder(testShape).build().numberOfCells());
    }

    @Test
    public void getNumberOfHashFunctionsTest() {
        assertEquals(2, StableShape.builder(testShape).build().getNumberOfHashFunctions());
    }

    @Test
    public void getShapeTest() {
        Shape shape = StableShape.builder(testShape).build().getShape();
        assertNotNull(shape);
        assertEquals(17, shape.getNumberOfBits());
        assertEquals(2, shape.getNumberOfHashFunctions());
        assertEquals(1.0 / 5, shape.getProbability(5), 0.01);
    }

    @Test
    public void stablePointTest() {
        assertEquals(0.526, StableShape.builder(testShape).build().stablePoint, 0.001);
    }
}
