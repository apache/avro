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
package org.apache.avro.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for AVRO-4253: the {@link FastReaderBuilder} record-reader
 * cache used to grow without bound when many distinct {@link Schema} instances
 * were used (e.g. a schema re-parsed for every file), because each cached
 * {@code RecordReader} strongly referenced the very {@link Schema} that keyed
 * it, defeating the weak-reference cache. The cache is now bounded (LRU).
 */
public class TestFastReaderBuilderCacheBounded {

  private static final String RECORD_SCHEMA = "{\"type\":\"record\",\"name\":\"CacheTest\",\"fields\":["
      + "{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}";

  @SuppressWarnings("unchecked")
  private static Map<Object, Object> readerCacheOf(FastReaderBuilder builder) throws Exception {
    Field field = FastReaderBuilder.class.getDeclaredField("readerCache");
    field.setAccessible(true);
    return (Map<Object, Object>) field.get(builder);
  }

  private static int cacheSize(Map<Object, Object> cache) {
    synchronized (cache) {
      return cache.size();
    }
  }

  /**
   * Simulate a long-running process that re-parses its schema for every message:
   * each iteration produces a fresh {@link Schema} identity. Before the fix this
   * grew the cache by one entry per iteration forever; now it stays bounded.
   */
  @Test
  public void cacheStaysBoundedAcrossDistinctSchemaInstances() throws Exception {
    FastReaderBuilder builder = new FastReaderBuilder(GenericData.get());
    Map<Object, Object> cache = readerCacheOf(builder);

    int iterations = FastReaderBuilder.DEFAULT_RECORD_READER_CACHE_SIZE + 1000;
    for (int i = 0; i < iterations; i++) {
      // A fresh parse yields a new Schema object identity every time.
      Schema schema = new Schema.Parser().parse(RECORD_SCHEMA);
      assertNotNull(builder.createDatumReader(schema));
    }

    int size = cacheSize(cache);
    assertTrue(size <= FastReaderBuilder.DEFAULT_RECORD_READER_CACHE_SIZE,
        "cache should be bounded but held " + size + " entries");
  }

  /** A stable schema instance reused across reads yields a stable, tiny cache. */
  @Test
  public void reusedSchemaInstanceHitsCache() throws Exception {
    FastReaderBuilder builder = new FastReaderBuilder(GenericData.get());
    Map<Object, Object> cache = readerCacheOf(builder);

    Schema schema = new Schema.Parser().parse(RECORD_SCHEMA);
    for (int i = 0; i < 1000; i++) {
      assertNotNull(builder.createDatumReader(schema));
    }

    assertEquals(1, cacheSize(cache), "reusing one schema instance must reuse one cache entry");
  }

  /**
   * Recursive schemas must still resolve correctly: the eviction guard never
   * removes an in-flight reader, so the recursive reference resolves to the same
   * instance instead of rebuilding endlessly.
   */
  @Test
  public void recursiveSchemaReadsCorrectly() throws Exception {
    Schema node = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"value\",\"type\":\"long\"},"
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");

    // Build chain: 1 -> 2 -> 3
    GenericRecord n3 = new GenericRecordBuilder(node).set("value", 3L).set("next", null).build();
    GenericRecord n2 = new GenericRecordBuilder(node).set("value", 2L).set("next", n3).build();
    GenericRecord n1 = new GenericRecordBuilder(node).set("value", 1L).set("next", n2).build();

    byte[] encoded = encode(node, n1);

    FastReaderBuilder builder = new FastReaderBuilder(GenericData.get());
    DatumReader<GenericRecord> reader = builder.createDatumReader(node);
    Decoder decoder = DecoderFactory.get().binaryDecoder(encoded, null);
    GenericRecord decoded = reader.read(null, decoder);

    assertEquals(1L, decoded.get("value"));
    GenericRecord next = (GenericRecord) decoded.get("next");
    assertEquals(2L, next.get("value"));
    GenericRecord last = (GenericRecord) next.get("next");
    assertEquals(3L, last.get("value"));
    assertNull(last.get("next"));
  }

  private static byte[] encode(Schema schema, GenericRecord record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
