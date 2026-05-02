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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestSchemaCache {

  private static final String SCHEMA_STRING = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"f\", \"type\": \"string\"}]}";

  @Test
  void testNoCache() {
    SchemaCache cache = SchemaCache.NO_CACHE;
    Schema schema1 = cache.getOrParseSchema(SCHEMA_STRING);
    Schema schema2 = cache.getOrParseSchema(SCHEMA_STRING);
    // Should be different instances since no caching
    assertNotSame(schema1, schema2);
    assertEquals(schema1, schema2);
  }

  @Test
  void testConcurrentCache() {
    SchemaCache cache = SchemaCache.createConcurrentCache();
    Schema schema1 = cache.getOrParseSchema(SCHEMA_STRING);
    Schema schema2 = cache.getOrParseSchema(SCHEMA_STRING);
    // Should be same instance due to caching
    assertSame(schema1, schema2);
  }

  @Test
  void testWeakCache() {
    SchemaCache.WeakSchemaCache cache = SchemaCache.createWeakCache();
    Schema schema1 = cache.getOrParseSchema(SCHEMA_STRING);
    Schema schema2 = cache.getOrParseSchema(SCHEMA_STRING);
    // Should be same instance due to caching
    assertSame(schema1, schema2);
    // Test trim method
    cache.trim(); // Should not throw
  }

  @Test
  void testWeakCacheAllowsGc() throws InterruptedException {
    SchemaCache.WeakSchemaCache cache = SchemaCache.createWeakCache();
    // Parse schema
    Schema schema = cache.getOrParseSchema(SCHEMA_STRING);
    assertEquals(1, cache.size(), "Cache should be 1 after fetch");
    // Hold a weak reference to it
    java.lang.ref.WeakReference<Schema> weakRef = new java.lang.ref.WeakReference<>(schema);
    // Clear strong reference
    schema = null;
    // Force GC

    for (int i = 0; i < 10 & !weakRef.isEnqueued(); i++) {
      System.gc();
      System.runFinalization();
      Thread.sleep(100); // Allow GC to run
    }
    assertNull(weakRef.get(), "Schema should have been GC'd");
    assertEquals(0, cache.size(), "Cache should be empty after GC");
    Schema retrieved = cache.getOrParseSchema(SCHEMA_STRING);
    assertNotNull(retrieved);
    assertEquals(1, cache.size(), "Cache should be 1 after fetch");
    // Now clear cache reference by trimming if possible, but since it's weak, after
    // GC it should be gone
    // But in practice, the cache holds the WeakReference, so the Schema is still
    // referenced.
    // To test GC, we need to not hold the retrieved reference.
    // This is hard to test reliably in unit tests.
    // For now, just test basic caching.
  }
}
