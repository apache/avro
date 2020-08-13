/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class TestUtf8 {
  @Test
  public void testByteConstructor() throws Exception {
    byte[] bs = "Foo".getBytes(StandardCharsets.UTF_8);
    Utf8 u = new Utf8(bs);
    assertEquals(bs.length, u.getByteLength());
    for (int i = 0; i < bs.length; i++) {
      assertEquals(bs[i], u.getBytes()[i]);
    }
  }

  @Test
  public void testArrayReusedWhenLargerThanRequestedSize() {
    byte[] bs = "55555".getBytes(StandardCharsets.UTF_8);
    Utf8 u = new Utf8(bs);
    assertEquals(5, u.getByteLength());
    byte[] content = u.getBytes();
    u.setByteLength(3);
    assertEquals(3, u.getByteLength());
    assertSame(content, u.getBytes());
    u.setByteLength(4);
    assertEquals(4, u.getByteLength());
    assertSame(content, u.getBytes());
  }

  @Test
  public void testHashCodeReused() {
    Utf8 u = new Utf8("a");
    assertEquals(97, u.hashCode());

    u.setByteLength(2);
    u.set("zz");

    assertEquals(97121, u.hashCode());
    assertEquals(97121, u.hashCode());
  }
}
