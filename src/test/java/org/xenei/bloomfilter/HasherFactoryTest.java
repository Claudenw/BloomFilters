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
package org.xenei.bloomfilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.util.function.ToLongBiFunction;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.xenei.bloomfilter.HasherFactory.Hasher;

public class HasherFactoryTest {

    @After
    public void teardown() {
        HasherFactory.reset();
    }

    @Test
    public void testListFuncs() {
        Set<String> names = HasherFactory.listFuncs();
        assertEquals(4, names.size());
        assertTrue(names.contains("MD5-SC"));
        assertTrue(names.contains("Murmur3_x64_128-SC"));
        assertTrue(names.contains("Murmur3_x86_32-SI"));
        assertTrue(names.contains("Objects32-SI"));
    }

    @Test
    public void testGetFunc_and_name() {
        Hasher h = HasherFactory.getHasher("MD5-SC");
        assertEquals("MD5-SC", h.getName());
    }

    @Test
    public void testRegisterAndReset() throws Exception {
        HasherFactory.register("TestFunc", TestFunc.class);
        Set<String> names = HasherFactory.listFuncs();
        assertEquals(5, names.size());
        assertTrue(names.contains("TestFunc"));
        HasherFactory.reset();
        names = HasherFactory.listFuncs();
        assertEquals(4, names.size());
        assertFalse(names.contains("TestFunc"));
    }

    public static class TestFunc implements ToLongBiFunction<ByteBuffer, Integer> {

        @Override
        public long applyAsLong(ByteBuffer buffer, Integer seed) {
            return seed;
        }

    }
}
