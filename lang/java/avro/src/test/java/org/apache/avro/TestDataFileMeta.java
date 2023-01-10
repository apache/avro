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
package org.apache.avro;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDataFileMeta {

  @TempDir
  public File DIR;

  @Test
  public void useReservedMeta() throws IOException {
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      assertThrows(AvroRuntimeException.class, () -> w.setMeta("avro.foo", "bar"));
    }
  }

  @Test
  public void useMeta() throws IOException {
    File f = new File(DIR, "testDataFileMeta.avro");
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      w.setMeta("hello", "bar");
      w.create(Schema.create(Type.NULL), f);
    }

    try (DataFileStream<Void> r = new DataFileStream<>(new FileInputStream(f), new GenericDatumReader<>())) {
      assertTrue(r.getMetaKeys().contains("hello"));

      assertEquals("bar", r.getMetaString("hello"));
    }

  }

  @Test
  public void useMetaAfterCreate() throws IOException {
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      w.create(Schema.create(Type.NULL), new ByteArrayOutputStream());
      assertThrows(AvroRuntimeException.class, () -> w.setMeta("foo", "bar"));
    }

  }

  @Test
  public void blockSizeSetInvalid() {
    int exceptions = 0;
    for (int i = -1; i < 33; i++) {
      // 33 invalid, one valid
      try {
        new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(i);
      } catch (IllegalArgumentException iae) {
        exceptions++;
      }
    }
    assertEquals(33, exceptions);
  }
}
