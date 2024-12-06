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
package org.apache.avro.file;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSeekableInputStream {
  @Test
  void readingData() throws IOException {
    byte[] data = "0123456789ABCD".getBytes(StandardCharsets.UTF_8);
    try (DataFileReader.SeekableInputStream sin = new DataFileReader.SeekableInputStream(
        new SeekableByteArrayInput(data))) {
      byte[] first8 = new byte[8];
      assertEquals(first8.length, sin.read(first8, 0, 8));
      assertArrayEquals("01234567".getBytes(StandardCharsets.UTF_8), first8);
      sin.seek(4);
      assertEquals(10, sin.available());
      assertEquals(2, sin.skip(2));
      assertEquals((byte) '6', sin.read());
      byte[] next4 = new byte[4];
      assertEquals(next4.length, sin.read(next4));
      assertArrayEquals("789A".getBytes(StandardCharsets.UTF_8), next4);
      assertEquals(11, sin.tell());
      assertEquals(data.length, sin.length());
    }
  }

  @Test
  void illegalSeek() throws IOException {
    try (SeekableInput in = new SeekableByteArrayInput("".getBytes(StandardCharsets.UTF_8));
        DataFileReader.SeekableInputStream sin = new DataFileReader.SeekableInputStream(in)) {
      Assert.assertThrows(IOException.class, () -> sin.seek(-5));
    }
  }
}
