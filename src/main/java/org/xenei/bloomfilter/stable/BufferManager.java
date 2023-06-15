package org.xenei.bloomfilter.stable;

import java.util.function.IntBinaryOperator;

public interface BufferManager {

    int get(int entry);

    void set(int entry);

    void decrement(int entry);

    boolean isSet(int entry);

    void clear();

    void func(int entry, int value, IntBinaryOperator f);

    BufferManager copy();
}