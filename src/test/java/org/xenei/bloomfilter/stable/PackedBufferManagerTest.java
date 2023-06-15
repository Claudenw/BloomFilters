package org.xenei.bloomfilter.stable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public class PackedBufferManagerTest {

    private AbstractBufferManager underTest;
    private int evenReset = 2; // 2 bits
    private int oddReset = 4; // 3 bits
    private Shape testShape = Shape.fromNP(5, 1.0 / 5);

    @Test
    public void lengthTest() {
        StableShape shape = StableShape.builder(testShape).setMax(evenReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        assertEquals(5, underTest.buffer.length);

        underTest = (AbstractBufferManager) AbstractBufferManager.instance(shape);
        assertEquals(5, underTest.buffer.length);

        shape = StableShape.builder(testShape).setMax(oddReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        assertEquals(9, underTest.buffer.length);

        underTest = (AbstractBufferManager) AbstractBufferManager.instance(shape);
        assertEquals(9, underTest.buffer.length);
    }

    @Test
    public void clearTest() {
        StableShape shape = StableShape.builder(testShape).setMax(evenReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.buffer[0] = (byte) 0xCC;
        underTest.buffer[1] = (byte) 0xFF;
        underTest.clear();
        assertThat(underTest.buffer).containsOnly(0);

        shape = StableShape.builder(testShape).setMax(oddReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.buffer[0] = (byte) 0xCC;
        underTest.buffer[1] = (byte) 0xFF;
        underTest.clear();
        assertThat(underTest.buffer).containsOnly(0);
    }

    @Test
    public void decrementEvenTest() {
        StableShape shape = StableShape.builder(testShape).setMax(evenReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.buffer[0] = (byte) 0xFF;
        underTest.buffer[1] = (byte) 5;
        underTest.decrement(1);
        assertEquals((byte) 0xFB, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xF7, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xF3, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);

        // show decrement at 0 does not do anything.
        underTest.decrement(1);
        assertEquals((byte) 0xF3, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);

        underTest.decrement(0);
        assertEquals((byte) 0xF2, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xF1, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xF0, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
    }

    @Test
    public void decrementOddTest() {
        StableShape shape = StableShape.builder(testShape).setMax(oddReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.buffer[0] = (byte) 0xFF;
        underTest.buffer[1] = (byte) 5;
        underTest.decrement(1);
        assertEquals((byte) 0xF7, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xEF, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xE7, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xDF, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xD7, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xCF, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(1);
        assertEquals((byte) 0xC7, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);

        // show decrement at 0 does not do anything.
        underTest.decrement(1);
        assertEquals((byte) 0xC7, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);

        underTest.decrement(0);
        assertEquals((byte) 0xC6, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xC5, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xC4, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xC3, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xC2, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xC1, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
        underTest.decrement(0);
        assertEquals((byte) 0xC0, underTest.buffer[0]);
        assertEquals((byte) 5, underTest.buffer[1]);
    }

    @Test
    public void funcTest() {
        StableShape shape = StableShape.builder(testShape).setMax(oddReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.func(0, 1, (x, y) -> x + y);
        assertEquals(1, underTest.buffer[0]);
        underTest.func(0, 1, (x, y) -> x + y);
        assertEquals(2, underTest.buffer[0]);
    }

    @Test
    public void getTest() {
        StableShape shape = StableShape.builder(testShape).setMax(oddReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.buffer[0] = (byte) 0x1C;
        underTest.buffer[1] = (byte) 5;
        assertEquals(4, underTest.get(0));
        assertEquals(3, underTest.get(1));
        assertEquals(5, underTest.get(2));
    }

    @Test
    public void isSetTest() {
        StableShape shape = StableShape.builder(testShape).setMax(oddReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.buffer[0] = (byte) 0xC5;
        underTest.buffer[1] = (byte) 5;
        assertTrue(underTest.isSet(0));
        assertFalse(underTest.isSet(1));
        assertTrue(underTest.isSet(2));
        assertFalse(underTest.isSet(3));
    }

    @Test
    public void setTest() {
        StableShape shape = StableShape.builder(testShape).setMax(oddReset).build();
        underTest = new AbstractBufferManager.Packed(shape);
        underTest.set(0);
        underTest.set(1);
        underTest.set(2);
        assertEquals(0x24, underTest.buffer[0]);
        assertEquals(4, underTest.buffer[1]);
    }
}
