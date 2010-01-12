/**
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
package org.apache.avro.io;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.junit.Test;


public class TestBinaryDecoder {
  /** Verify EOFException throw at EOF */

  @Test(expected=EOFException.class)
  public void testEOFBoolean() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).readBoolean();
  }
  
  @Test(expected=EOFException.class)
  public void testEOFInt() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).readInt();
  }
  
  @Test(expected=EOFException.class)
  public void testEOFLong() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).readLong();
  }
  
  @Test(expected=EOFException.class)
  public void testEOFFloat() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).readFloat();
  }
  
  @Test(expected=EOFException.class)
  public void testEOFDouble() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).readDouble();
  }
  
  @Test(expected=EOFException.class)
  public void testEOFBytes() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).readBytes(null);
  }
  
  @Test(expected=EOFException.class)
  public void testEOFString() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).
      readString(new Utf8("a"));
  }
  
  @Test(expected=EOFException.class)
  public void testEOFFixed() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).
      readFixed(new byte[1]);
  }

  @Test(expected=EOFException.class)
  public void testEOFEnum() throws IOException {
    new BinaryDecoder(new ByteArrayInputStream(new byte[0])).readEnum();
  }
  
}
