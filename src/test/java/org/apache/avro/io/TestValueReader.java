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
import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.avro.util.Utf8;


public class TestValueReader extends TestCase {
  /** Verify EOFException throw at EOF */
  public void testEOFHandling() throws IOException {
    InputStream is = new ByteArrayInputStream(new byte[0]);
    ValueReader vr = new ValueReader(is);

    try {
      vr.readBoolean();
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readBuffer(null);
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readBytes(new byte[1]);
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readBytes(new byte[1], 0, 1);
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readDouble();
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readFloat();
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readInt();
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readLong();
      fail();
    } catch (EOFException e) { /* this is good */ }
    try {
      vr.readUtf8(new Utf8("a"));
      fail();
    } catch (EOFException e) { /* this is good */ }
  }
}
