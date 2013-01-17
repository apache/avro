/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import java.util.Random;

import java.io.ByteArrayOutputStream;

import org.junit.Assert;
import org.junit.Test;

public class TestIOBuffers {

  private static final int COUNT = 1000;

  @Test public void testEmpty() throws Exception {
    OutputBuffer out = new OutputBuffer();
    ByteArrayOutputStream temp = new ByteArrayOutputStream();
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    Assert.assertEquals(0, in.tell());
    Assert.assertEquals(0, in.length());
  }

  @Test public void testZero() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    out.writeInt(0);
    byte[] bytes = out.toByteArray();
    Assert.assertEquals(1, bytes.length);
    Assert.assertEquals(0, bytes[0]);
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    Assert.assertEquals(0, in.readInt());
  }

  @Test public void testInt() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeInt(random.nextInt());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextInt(), in.readInt());
  }

  @Test public void testLong() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeLong(random.nextLong());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextLong(), in.readLong());
  }

  @Test public void testFixed32() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFixed32(random.nextInt());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextInt(), in.readFixed32());
  }

  @Test public void testFixed64() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFixed64(random.nextLong());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextLong(), in.readFixed64());
  }
  
  @Test public void testFloat() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFloat(random.nextFloat());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextFloat(), in.readFloat(), 0);
  }
  
  @Test public void testDouble() throws Exception {
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeDouble(Double.MIN_VALUE);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(Double.MIN_VALUE, in.readDouble(), 0);
  }
  
  @Test public void testBytes() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeBytes(TestUtil.randomBytes(random));
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(TestUtil.randomBytes(random), in.readBytes(null));
  }

  @Test public void testString() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeString(TestUtil.randomString(random));
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(TestUtil.randomString(random), in.readString());
  }
  @Test public void testSkipNull() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(null, ValueType.NULL);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.NULL);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipInt() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Integer.MAX_VALUE, ValueType.INT);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.INT);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipLong() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Long.MAX_VALUE, ValueType.LONG);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.LONG);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipFixed32() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Integer.MAX_VALUE, ValueType.FIXED32);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.LONG);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipFixed64() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Long.MAX_VALUE, ValueType.FIXED64);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.LONG);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipFloat() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Float.MAX_VALUE, ValueType.FLOAT);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.FLOAT);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipDouble() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Double.MAX_VALUE, ValueType.DOUBLE);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.DOUBLE);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipString() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue("trevni", ValueType.STRING);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.STRING);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testSkipBytes() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue("trevni".getBytes(), ValueType.BYTES);
    out.writeLong(sentinel);
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.skipValue(ValueType.BYTES);
    Assert.assertEquals(sentinel, in.readLong());
  }
  @Test public void testInitPos() throws Exception {
    long sentinel = Long.MAX_VALUE;
    OutputBuffer out = new OutputBuffer();
    out.writeValue(Integer.MAX_VALUE, ValueType.INT);
    out.writeLong(sentinel);
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    in.readInt();
    long pos = in.tell();
    in = new InputBuffer(new InputBytes(out.toByteArray()), pos);
    Assert.assertEquals(sentinel, in.readLong());
  }
}
