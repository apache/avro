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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.junit.Test;

public class TestPolymorphicEncoding {

  @Test
  public void testPolymorphicEncoding() throws IOException {
    List<Animal> expected = Arrays.asList(new Cat("Green"), new Dog(5));
    byte[] encoded = write(Animal.class, expected);
    List<Animal> decoded = read(encoded);

    assertEquals(expected, decoded);
  }

  private <T> List<T> read(byte[] toDecode) throws IOException {
    DatumReader<T> datumReader = new ReflectDatumReader<>();
    try (DataFileStream<T> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(toDecode, 0, toDecode.length),
        datumReader);) {
      List<T> toReturn = new ArrayList<>();
      while (dataFileReader.hasNext()) {
        toReturn.add(dataFileReader.next());
      }
      return toReturn;
    }
  }

  private <T> byte[] write(Class<?> type, List<T> custom) {
    Schema schema = ReflectData.get().getSchema(type);
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, baos);
      for (T c : custom) {
        writer.append(c);
      }
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static sealed interface Animal permits Cat,Dog {
  }

  public static record Dog(int size) implements Animal {
  }

  public static record Cat(String color) implements Animal {
  }

}
