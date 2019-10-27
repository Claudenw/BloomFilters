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
package org.xenei.bloomfilter.hasher;

import java.nio.ByteBuffer;
import java.util.function.ToLongBiFunction;

import org.xenei.bloomfilter.hasher.MurmurHash3.LongPair;

/**
 * An implementation of ToLongBiFunction<ByteBuffer, Integer> that
 * performs Murmur128 hashing using a signed cyclic method.
 *
 */
public class Murmur128 implements ToLongBiFunction<ByteBuffer, Integer> {
    /**
     * The result of the hash 0 call.
     */
    private LongPair parts = null;

    /**
     * The name of this hash method.
     */
    public static final String name = "Murmur3_x64_128-SC";

    @Override
    public long applyAsLong(ByteBuffer buffer, Integer seed) {
        if (parts == null || seed == 0) {
            parts = new LongPair();
            MurmurHash3.murmurhash3_x64_128(buffer, 0, buffer.limit(), 0, parts);
        } else {
            parts.val1 += parts.val2;
        }
        return parts.val1;
    }

}
