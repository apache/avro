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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.junit.Test;

/**
 * Simulates a record in behavior only. This way it can run on all supported
 * version of java To simulate has a class with final variables and a single
 * constructor matching variables This class has the RecordEncoding annotation
 * attached
 *
 */
public class TestJavaRecordEncoder {

  @Test
  public void testEncoderOnTopLevelField() throws IOException {
    Custom in = new Custom("hello world");
    byte[] encoded = write(in);
    Custom decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.getField());
  }

  private <T> T read(byte[] toDecode) throws IOException {
    DatumReader<T> datumReader = new ReflectDatumReader<>();
    try (DataFileStream<T> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(toDecode, 0, toDecode.length),
        datumReader);) {
      dataFileReader.hasNext();
      return dataFileReader.next();
    }
  }

  private <T> byte[] write(T custom) {
    Schema schema = ReflectData.get().getSchema(custom.getClass());
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, baos);
      writer.append(custom);
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @AvroEncode(using = ReflectRecordEncoding.class)
  public static class Custom {

    private final String field;

    public Custom(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }
  }

}
