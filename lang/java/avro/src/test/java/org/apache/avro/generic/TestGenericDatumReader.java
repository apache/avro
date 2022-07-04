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
package org.apache.avro.generic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGenericDatumReader {

  private static final Random r = new Random(System.currentTimeMillis());

  @Test
  public void testReaderCache() {
    final GenericDatumReader.ReaderCache cache = new GenericDatumReader.ReaderCache(this::findStringClass);
    List<Thread> threads = IntStream.rangeClosed(1, 200).mapToObj((int index) -> {
      final Schema schema = TestGenericDatumReader.this.build(index);
      final WithSchema s = new WithSchema(schema, cache);
      return (Runnable) () -> s.test();
    }).map(Thread::new).collect(Collectors.toList());
    threads.forEach(Thread::start);
    threads.forEach((Thread t) -> {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testNewInstanceFromString() {
    final GenericDatumReader.ReaderCache cache = new GenericDatumReader.ReaderCache(this::findStringClass);

    Object object = cache.newInstanceFromString(StringBuilder.class, "Hello");
    assertEquals(StringBuilder.class, object.getClass());
    StringBuilder builder = (StringBuilder) object;
    assertEquals("Hello", builder.toString());

  }

  static class WithSchema {
    private final Schema schema;

    private final GenericDatumReader.ReaderCache cache;

    public WithSchema(Schema schema, GenericDatumReader.ReaderCache cache) {
      this.schema = schema;
      this.cache = cache;
    }

    public void test() {
      this.cache.getStringClass(schema);
    }
  }

  private List<Schema> list = new ArrayList<>();

  private Schema build(int index) {
    int schemaNum = (index - 1) % 50;
    if (index <= 50) {
      Schema schema = Schema.createRecord("record_" + schemaNum, "doc", "namespace", false,
          Arrays.asList(new Schema.Field("field" + schemaNum, Schema.create(Schema.Type.STRING))));
      list.add(schema);
    }

    return list.get(schemaNum);
  }

  private Class findStringClass(Schema schema) {
    this.sleep();
    if (schema.getType() == Schema.Type.INT) {
      return Integer.class;
    }
    if (schema.getType() == Schema.Type.STRING) {
      return String.class;
    }
    if (schema.getType() == Schema.Type.LONG) {
      return Long.class;
    }
    if (schema.getType() == Schema.Type.FLOAT) {
      return Float.class;
    }
    return String.class;
  }

  private void sleep() {
    long timeToSleep = r.nextInt(30) + 10L;
    if (timeToSleep > 25) {
      try {
        Thread.sleep(timeToSleep);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
