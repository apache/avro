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
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Isolated
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
    // the isnt really a guarantee that its cleared by now, but is seems ok fo the
    // unit test
    // at least.
    // So by now we expect the GC to have cleared the schema from the cache,
    // so the weak reference should be null and the cache should be empty (or more
    // accurately become empty when checked).
    assertNull(weakRef.get(), "Schema should have been GC'd");
    assertEquals(0, cache.size(), "Cache should be empty after GC");
    // and now it should be able to parse again and cache it again
    Schema retrieved = cache.getOrParseSchema(SCHEMA_STRING);
    assertNotNull(retrieved);
    assertEquals(1, cache.size(), "Cache should be 1 after fetch");
  }

  @Test
  void testSoftCacheAllowsGc() throws InterruptedException {
    SchemaCache.SoftSchemaCache cache = SchemaCache.createSoftCache();
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
    // the isnt really a guarantee that its cler or not cleared by now, but is seems
    // ok fo the unit test
    // at least.
    // So by now we expect the GC to have not cleared the schema from the cache, as
    // there isnt any pressure
    // so the weak reference should be not null and the cache should not be empty.

    assertNotNull(weakRef.get(), "Schema should not have been GC'd, its soft and there is no pressure");
    assertEquals(1, cache.size(), "Cache should not be empty after GC");
    Schema retrieved = cache.getOrParseSchema(SCHEMA_STRING);
    assertNotNull(retrieved);
    assertEquals(1, cache.size(), "Cache should be 1 after fetch");
    retrieved = null;
    // Now we can try to create some memory pressure to encourage GC to clear the
    // soft reference.
    try {
      List<Object> allMemory = new ArrayList<>(20000);
      while (true) {
        allMemory.add(new byte[1024 * 1024 * 1024]); // Allocate 1GB chunks
      }
    } catch (OutOfMemoryError e) {
    }
    assertEquals(0, cache.size(), "Cache should not be empty after GC");

  }
}
