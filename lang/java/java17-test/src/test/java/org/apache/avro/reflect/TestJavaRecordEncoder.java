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

package org.apache.avro.reflect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileWriter.AppendWriteException;
import org.apache.avro.io.DatumReader;
import org.junit.Test;

import static org.apache.avro.reflect.RecordReadWriteUtil.read;
import static org.apache.avro.reflect.RecordReadWriteUtil.write;

public class TestJavaRecordEncoder {

  @Test
  public void testRecord() throws IOException {
    Custom in = new Custom("hello world");
    byte[] encoded = write(in);
    Custom decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
  }

  @Test
  public void testRecordWithNulls() throws IOException {
    var in = new CustomWithNull("hello world", null);
    byte[] encoded = write(in);
    CustomWithNull decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.field());
    assertEquals(null, decoded.field2());
  }

  @Test
  public void testNonNullErrors() throws IOException {
    var in = new CustomWithNull(null, "pass");
    assertThrows(AppendWriteException.class, () -> {
      write(in);
    });
  }

  public record Custom(String field) {
  }

  public record CustomWithNull(String field, @Nullable String field2) {
  }

}
