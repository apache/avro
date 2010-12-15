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
package org.apache.avro.file;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;

public class TestLengthLimitedInputStream {
  InputStream raw;

  @Before
  public void setupRawStream() {
    byte[] buf = new byte[128];
    for (int i = 0; i < 128; ++i) {
      buf[i] = (byte)i;
    }
    raw = new ByteArrayInputStream(buf);
  }

  @Test
  public void testAvailable() throws IOException {
    InputStream is = new LengthLimitedInputStream(raw, 10);
    assertEquals(10, is.available());
    is.skip(100);
    assertEquals(0, is.available());
  }

  @Test
  public void testRead() throws IOException {
    InputStream is = new LengthLimitedInputStream(raw, 10);
    byte[] x = new byte[12];
    assertEquals(0, is.read());
    assertEquals(9, is.read(x));
    assertEquals(-1, is.read(x));
    assertEquals(x[8], 9);
  }
}
