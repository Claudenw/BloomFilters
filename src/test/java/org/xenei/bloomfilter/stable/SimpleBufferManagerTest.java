package org.xenei.bloomfilter.stable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public class SimpleBufferManagerTest {

    private AbstractBufferManager underTest;

    // reset value of 128 ensures simple implementation
    private StableShape shape = StableShape.builder(Shape.fromNP(5, 1.0 / 5)).setMax(128).build();

    @Test
    public void lengthTest() {
        underTest = new AbstractBufferManager.Simple(shape);
        assertEquals(17, underTest.buffer.length);
    }

    @Test
    public void clearTest() {
        underTest = new AbstractBufferManager.Simple(shape);
        underTest.buffer[1] = (byte) 0xFF;
        underTest.buffer[3] = (byte) 0xFF;
        underTest.clear();
        assertThat(underTest.buffer).containsOnly(0);
    }

    @Test
    public void decrementTest() {
        underTest = new AbstractBufferManager.Simple(shape);
        underTest.buffer[1] = (byte) 0xFF;
        underTest.buffer[3] = (byte) 5;
        underTest.decrement(1);
        assertThat(underTest.buffer).contains(0, (byte) 0xFE, 0, 5, 0);
        underTest.decrement(0);
        assertThat(underTest.buffer).contains(0, (byte) 0xFE, 0, 5, 0);
        underTest.decrement(3);
        assertThat(underTest.buffer).contains(0, (byte) 0xFE, 0, 4, 0);
    }

    @Test
    public void funcTest() {
        underTest = new AbstractBufferManager.Simple(shape);
        underTest.func(0, 1, (x, y) -> x + y);
        assertEquals(1, underTest.buffer[0]);
        underTest.func(0, 1, (x, y) -> x + y);
        assertEquals(2, underTest.buffer[0]);

    }

    @Test
    public void getTest() {
        underTest = new AbstractBufferManager.Simple(shape);
        underTest.buffer[1] = (byte) 0xFF;
        underTest.buffer[3] = (byte) 5;
        assertEquals(0, underTest.get(0));
        assertEquals(0xFF, underTest.get(1));
        assertEquals(5, underTest.get(3));
    }

    @Test
    public void isSetTest() {
        underTest = new AbstractBufferManager.Simple(shape);
        underTest.buffer[1] = (byte) 0xFF;
        underTest.buffer[3] = (byte) 5;
        assertFalse(underTest.isSet(0));
        assertTrue(underTest.isSet(1));
        assertFalse(underTest.isSet(2));
        assertTrue(underTest.isSet(3));
        assertFalse(underTest.isSet(4));
    }

    @Test
    public void setTest() {
        underTest = new AbstractBufferManager.Simple(shape);
        underTest.set(1);
        underTest.set(3);
        assertThat(underTest.buffer).contains(0, 128, 0, 128, 0);
    }
}
