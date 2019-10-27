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
import java.nio.LongBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.ToLongBiFunction;

/**
 * An implementation of ToLongBiFunction<ByteBuffer, Integer> that
 * performs MD5 hashing using a signed cyclic method.
 *
 */
public class MD5 implements ToLongBiFunction<ByteBuffer, Integer> {

    /**
     * The MD5 digest implementation.
     */
    private MessageDigest md;

    /**
     * The result from the digest 0
     */
    private long[] result = null;

    /**
     * The name of this hash function.
     */
    public static final String name = "MD5-SC";

    /**
     * Constructs the MD5 hashing function.
     * @throws NoSuchAlgorithmException on internal error.
     */
    public MD5() throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("MD5");
    }

    @Override
    public long applyAsLong(ByteBuffer buffer, Integer seed) {

        if (result == null || seed == 0) {
            result = new long[2];
            byte[] hash;
            synchronized (md) {
                md.update(buffer.duplicate().position(0));
                hash = md.digest();
                md.reset();
            }

            LongBuffer lb = ByteBuffer.wrap(hash).asLongBuffer();
            lb.position(0);
            System.out.println(lb.limit());
            result[0] = lb.get(0);
            result[1] = lb.get(1);
        } else {
            result[0] += result[1];
        }
        return result[0];
    }

}
