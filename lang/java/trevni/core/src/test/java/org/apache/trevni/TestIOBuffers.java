/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;

import java.io.ByteArrayOutputStream;

import org.junit.jupiter.api.Test;

public class TestIOBuffers {

  private static final int COUNT = 1001;

  @Test
  void empty() throws Exception {
    OutputBuffer out = new OutputBuffer();
    ByteArrayOutputStream temp = new ByteArrayOutputStream();
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    assertEquals(0, in.tell());
    assertEquals(0, in.length());
    out.close();
  }

  @Test
  void zero() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    out.writeInt(0);
    byte[] bytes = out.toByteArray();
    assertEquals(1, bytes.length);
    assertEquals(0, bytes[0]);
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    assertEquals(0, in.readInt());
    out.close();
  }

  @Test
  void testBoolean() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeValue(random.nextBoolean(), ValueType.BOOLEAN);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(random.nextBoolean(), in.readValue(ValueType.BOOLEAN));
    out.close();
  }

  @Test
  void testInt() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeInt(random.nextInt());

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(random.nextInt(), in.readInt());
    out.close();
  }

  @Test
  void testLong() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeLong(random.nextLong());

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(random.nextLong(), in.readLong());
    out.close();
  }

  @Test
  void fixed32() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFixed32(random.nextInt());

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(random.nextInt(), in.readFixed32());
    out.close();
  }

  @Test
  void fixed64() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFixed64(random.nextLong());

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(random.nextLong(), in.readFixed64());
    out.close();
  }

  @Test
  void testFloat() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFloat(random.nextFloat());

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(random.nextFloat(), in.readFloat(), 0);
    out.close();
  }

  @Test
  void testDouble() throws Exception {
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeDouble(Double.MIN_VALUE);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    for (int i = 0; i < COUNT; i++)
      assertEquals(Double.MIN_VALUE, in.readDouble(), 0);
    out.close();
  }

  @Test
  void bytes() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeBytes(TestUtil.randomBytes(random));

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(TestUtil.randomBytes(random), in.readBytes(null));
    out.close();
  }

  @Test
  void string() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeString(TestUtil.randomString(random));

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      assertEquals(TestUtil.randomString(random), in.readString());
    out.close();
  }

  @Test
  void skipNull() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(null, ValueType.NULL);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.NULL);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipBoolean() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(false, ValueType.BOOLEAN);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.BOOLEAN);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipInt() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Integer.MAX_VALUE, ValueType.INT);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.INT);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipLong() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Long.MAX_VALUE, ValueType.LONG);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.LONG);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipFixed32() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Integer.MAX_VALUE, ValueType.FIXED32);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.LONG);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipFixed64() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Long.MAX_VALUE, ValueType.FIXED64);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.LONG);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipFloat() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Float.MAX_VALUE, ValueType.FLOAT);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.FLOAT);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipDouble() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Double.MAX_VALUE, ValueType.DOUBLE);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.DOUBLE);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipString() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue("trevni", ValueType.STRING);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.STRING);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void skipBytes() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue("trevni".getBytes(UTF_8), ValueType.BYTES);
    out.writeLong(sentinel);

    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.BYTES);
    assertEquals(sentinel, in.readLong());
    out.close();
  }

  @Test
  void initPos() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Integer.MAX_VALUE, ValueType.INT);
    out.writeLong(sentinel);
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.readInt();
    long pos = in.tell();
    in = new InputBuffer(new InputBytes(out.toByteArray()), pos);
    assertEquals(sentinel, in.readLong());
    out.close();
  }
}
