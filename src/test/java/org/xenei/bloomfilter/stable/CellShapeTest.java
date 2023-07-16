package org.xenei.bloomfilter.stable;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.commons.collections4.bloomfilter.Shape;
import org.junit.jupiter.api.Test;

public class CellShapeTest {

    private Shape baseShape = Shape.fromNM(10, 128);
    
    private void verify(CellShape shape, long mask, int bitsPerCell, int maxValue ) {
        assertEquals(mask,shape.cellMask());
        assertEquals(bitsPerCell, shape.getBitsPerCell());
        assertEquals(maxValue, shape.maxValue());
    }
    
    @Test
    public void testFromMaxValue() {
        CellShape shape = CellShape.fromMaxValue(baseShape, 3);
        verify( shape, 0x3, 2, 3 );
        shape = CellShape.fromMaxValue(baseShape, 5);
        verify( shape, 0x7, 3, 5 );
        
    }
}
