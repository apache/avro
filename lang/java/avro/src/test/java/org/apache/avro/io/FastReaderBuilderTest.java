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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Memory leak tests for {@link FastReaderBuilder}.
 *
 * <p>
 * These tests verify that the cache eviction mechanism works correctly and prevents memory leaks as described in
 * AVRO-4253. The issue occurs when {@code RecordReader} holds strong references to Schema objects that are cached as
 * weak keys in {@code WeakIdentityHashMap}, preventing garbage collection.
 *
 * <p>
 * The fix involves deep-cloning schemas when {@code readerSchema == writerSchema} (same object identity), ensuring that
 * the cached {@code RecordReader} holds a reference to the cloned schema rather than the original weak key, allowing
 * the original to be garbage collected.
 */
@Isolated
class TestFastReaderBuilder {

  private static final Field READER_CACHE_FIELD;
  private static final Field READER_SIMPLE_CACHE_FIELD;

  static {
    try {
      READER_CACHE_FIELD = FastReaderBuilder.class.getDeclaredField("readerCache");
      READER_CACHE_FIELD.setAccessible(true);

      READER_SIMPLE_CACHE_FIELD = FastReaderBuilder.class.getDeclaredField("readerSimpleCache");
      READER_SIMPLE_CACHE_FIELD.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private FastReaderBuilder builder;

  @BeforeEach
  void setUp() {
    builder = new FastReaderBuilder(GenericData.get());
  }

  // ============================================================================
  // Tests for Identical Schemas (reader == writer)
  // ============================================================================

  /**
   * Tests for the case where reader and writer schemas are the same object. This uses the readerSimpleCache
   * (soft-referenced) to prevent memory leaks.
   */
  @Nested
  class WhenSchemasIdentical {
    void forceGCAndReap() throws Exception {
      forceGCAndReapUntil(() -> getSimpleCacheSize() == 0);
    }

    @Test
    void testCacheEviction() throws Exception {
      Schema schema = buildNestedWriterSchema(0);
      String schemaJson = schema.toString();

      List<WeakReference<Schema>> schemaRefs = new ArrayList<>();

      // Create 5 identical schemas (different objects, same structure)
      for (int i = 0; i < 5; i++) {
        Schema freshSchema = new Schema.Parser().parse(schemaJson);
        schemaRefs.add(new WeakReference<>(freshSchema));

        // This calls createDatumReader(schema, schema) where both args are the same object
        DatumReader<?> reader = builder.createDatumReader(freshSchema);
        assertNotNull(reader, "Reader should be created");

        // Discard references
        reader = null;
        freshSchema = null;
      }

      forceClearSoftReferences();
      forceGCAndReap();

      // Count how many schemas were garbage collected
      int collectedCount = 0;
      for (WeakReference<Schema> ref : schemaRefs) {
        if (ref.get() == null) {
          collectedCount++;
        }
      }

      // Verify both caches are cleared
      assertTrue(collectedCount > 0,
          "At least some schemas should be garbage collected. "
              + collectedCount + " / " + schemaRefs.size() + " collected");
      assertAllCachesCleared();
    }

    private void assertAllCachesCleared() throws Exception {
      assertEquals(0, getSimpleCacheSize(), "Simple cache should be cleared after GC");
      assertEquals(0, getOuterCacheSize(), "Outer cache should not be used");
      assertEquals(0, getInnerCacheSize(), "Inner cache should not be used");
    }

    @Test
    void testBothCachesClearedAfterGC() throws Exception {
      Schema schema = buildNestedWriterSchema(0);
      String schemaJson = schema.toString();

      // Create readers for multiple identical schemas
      for (int i = 0; i < 3; i++) {
        Schema freshSchema = new Schema.Parser().parse(schemaJson);
        DatumReader<?> reader = builder.createDatumReader(freshSchema);
        assertNotNull(reader);
        reader = null;
        freshSchema = null;
      }

      // Verify caches have entries
      assertTrue(getSimpleCacheSize() > 0, "Simple cache should have entries");

      // Force GC and clear soft references
      forceClearSoftReferences();
      forceGCAndReap();

      // Verify both caches are cleared
      assertAllCachesCleared();
    }

    @Test
    void testCache() throws Exception {
      Schema baseSchema = buildNestedWriterSchema(0);
      DatumReader<?> reader1 = builder.createDatumReader(baseSchema);
      assertNotNull(reader1);

      DatumReader<?> reader2 = builder.createDatumReader(baseSchema);
      assertNotNull(reader2);

      assertSame(reader1, reader2);
    }

    @Test
    void testRepeatedReaderCreationMemoryBounded() throws Exception {
      Schema baseSchema = buildNestedWriterSchema(0);
      String schemaJson = baseSchema.toString();

      // Create many readers with independently parsed identical schemas
      for (int i = 0; i < 30; i++) {
        Schema freshSchema = new Schema.Parser().parse(schemaJson);
        DatumReader<?> reader = builder.createDatumReader(freshSchema);
        assertNotNull(reader);

        reader = null;
        freshSchema = null;

        if (i % 10 == 0) {
          forceGCAndReap();
        }
      }

      forceClearSoftReferences();
      forceGCAndReap();

      // Verify both caches are cleared
      assertAllCachesCleared();
    }

    @Test
    void testNestedIdenticalSchemasCleared() throws Exception {
      Schema deepSchema = buildNestedWriterSchema(0);
      String schemaJson = deepSchema.toString();

      // Create readers for deeply nested schemas
      for (int i = 0; i < 5; i++) {
        Schema freshSchema = new Schema.Parser().parse(schemaJson);
        DatumReader<?> reader = builder.createDatumReader(freshSchema);
        assertNotNull(reader);
      }

      forceClearSoftReferences();
      forceGCAndReap();

      // Verify both caches are cleared
      assertAllCachesCleared();
    }
  }

  // ============================================================================
  // Tests for Different Schemas (reader != writer)
  // ============================================================================

  /**
   * Tests for the case where reader and writer schemas are different objects. This uses the readerCache (weak
   * references) which should allow eviction when schemas are no longer externally referenced.
   */
  @Nested
  class WhenSchemasDifferent {

    private void assertAllCachesCleared() throws Exception {
      assertEquals(0, getSimpleCacheSize(), "Simple cache should not be used");
      assertEquals(0, getOuterCacheSize(), "Outer cache should should be cleared after GC");
      assertEquals(0, getInnerCacheSize(), "Inner cache should should be cleared after GC");
    }

    void forceGCAndReap() throws Exception {
      forceGCAndReapUntil(() -> getOuterCacheSize() + getInnerCacheSize() == 0);
    }

    @Test
    void testCacheEviction() throws Exception {
      Schema baseWriterSchema = buildNestedWriterSchema(0);
      String writerSchemaJson = baseWriterSchema.toString();

      Schema baseReaderSchema = buildNestedReaderSchema(0);
      String readerSchemaJson = baseReaderSchema.toString();

      List<WeakReference<Schema>> writerRefs = new ArrayList<>();
      List<WeakReference<Schema>> readerRefs = new ArrayList<>();

      // Create readers with different writer and reader schemas
      for (int i = 0; i < 5; i++) {
        Schema writerSchema = new Schema.Parser().parse(writerSchemaJson);
        Schema readerSchema = new Schema.Parser().parse(readerSchemaJson);
        writerRefs.add(new WeakReference<>(writerSchema));
        readerRefs.add(new WeakReference<>(readerSchema));

        DatumReader<?> reader = builder.createDatumReader(writerSchema, readerSchema);
        assertNotNull(reader, "Reader should be created");
      }

      forceGCAndReap();

      // Count collected schemas
      int collectedWriters = 0;
      int collectedReaders = 0;
      for (WeakReference<Schema> ref : writerRefs) {
        if (ref.get() == null) collectedWriters++;
      }
      for (WeakReference<Schema> ref : readerRefs) {
        if (ref.get() == null) collectedReaders++;
      }

      // Both sets of schemas should be evictable
      assertTrue(collectedWriters > 0, "Writer schemas should be garbage collected");
      assertTrue(collectedReaders > 0, "Reader schemas should be garbage collected");

      // Verify both caches are cleared
      assertAllCachesCleared();
    }
    @Test
    void testCache() throws Exception {
      Schema baseWriterSchema = buildNestedWriterSchema(0);

      Schema baseReaderSchema = buildNestedReaderSchema(0);

      DatumReader<?> reader1 = builder.createDatumReader(baseWriterSchema, baseReaderSchema);
      assertNotNull(reader1);

      DatumReader<?> reader2 = builder.createDatumReader(baseWriterSchema, baseReaderSchema);
      assertNotNull(reader2);

      assertSame(reader1, reader2);
    }

    @Test
    void testBothCachesClearedWithDifferentSchemas() throws Exception {
      Schema baseWriterSchema = buildNestedWriterSchema(0);
      String writerSchemaJson = baseWriterSchema.toString();

      Schema baseReaderSchema = buildNestedReaderSchema(0);
      String readerSchemaJson = baseReaderSchema.toString();

      // Create readers with different schemas
      for (int i = 0; i < 3; i++) {
        Schema writerSchema = new Schema.Parser().parse(writerSchemaJson);
        Schema readerSchema = new Schema.Parser().parse(readerSchemaJson);
        DatumReader<?> reader = builder.createDatumReader(writerSchema, readerSchema);
        assertNotNull(reader);
      }

      // Verify caches have entries
      assertTrue(getOuterCacheSize() > 0, "Outer cache should have entries");
      assertTrue(getInnerCacheSize() > 0, "Inner cache should have entries");

      // Force GC and reap
      forceGCAndReap();

      // Verify both caches are cleared
      assertAllCachesCleared();
    }

    @Test
    void testRepeatedDifferentSchemasMemoryBounded() throws Exception {
      Schema baseWriterSchema = buildNestedWriterSchema(0);
      String writerSchemaJson = baseWriterSchema.toString();

      Schema baseReaderSchema = buildNestedReaderSchema(0);
      String readerSchemaJson = baseReaderSchema.toString();

      // Create many readers with different schemas
      for (int i = 0; i < 30; i++) {
        Schema writerSchema = new Schema.Parser().parse(writerSchemaJson);
        Schema readerSchema = new Schema.Parser().parse(readerSchemaJson);
        DatumReader<?> reader = builder.createDatumReader(writerSchema, readerSchema);
        assertNotNull(reader);

        reader = null;
        writerSchema = null;
        readerSchema = null;

        if (i % 10 == 0) {
          forceGCAndReap();
        }
      }

      forceGCAndReap();

      // Verify both caches are cleared
      assertAllCachesCleared();
    }

    @Test
    void testNestedDifferentSchemasCleared() throws Exception {
      Schema baseWriterSchema = buildNestedWriterSchema(0);
      String writerSchemaJson = baseWriterSchema.toString();

      Schema baseReaderSchema = buildNestedReaderSchema(0);
      String readerSchemaJson = baseReaderSchema.toString();

      // Create readers with deeply nested different schemas
      for (int i = 0; i < 5; i++) {
        Schema writerSchema = new Schema.Parser().parse(writerSchemaJson);
        Schema readerSchema = new Schema.Parser().parse(readerSchemaJson);
        DatumReader<?> reader = builder.createDatumReader(writerSchema, readerSchema);
        assertNotNull(reader);
      }

      forceGCAndReap();

      // Verify all caches are cleared
      // Verify both caches are cleared
      assertAllCachesCleared();
    }
  }

  /**
   * Build a nested record schema with specified depth. Each level has 10 string fields and an optional child record.
   */
  private Schema buildNestedWriterSchema(int level) {
    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("Level_" + level)
        .namespace("org.apache.avro.test.retention")
        .fields();

    for (int f = 0; f < 10; f++) {
      fields = fields.requiredString("field_" + f);
    }

    if (level < 3) {
      Schema childSchema = buildNestedWriterSchema(level + 1);
      fields = fields.name("child").type(childSchema).noDefault();
    }

    return fields.endRecord();
  }
  /**
   * Build a nested record schema with specified depth. Each level has 5 string fields and an optional child record.
   */
  private Schema buildNestedReaderSchema(int level) {
    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("Level_" + level)
        .namespace("org.apache.avro.test.retention")
        .fields();

    for (int f = 0; f < 5; f++) {
      fields = fields.requiredString("field_" + f);
    }

    if (level < 3) {
      Schema childSchema = buildNestedReaderSchema(level + 1);
      fields = fields.name("child").type(childSchema).noDefault();
    }

    return fields.endRecord();
  }

  /**
   * Get the number of entries in the outer WeakIdentityHashMap (one per unique reader schema).
   */
  @SuppressWarnings("unchecked")
  private int getOuterCacheSize() {
    try {
      Map<Schema, Map<Schema, ?>> cache = (Map<Schema, Map<Schema, ?>>) READER_CACHE_FIELD.get(builder);
      return cache.size();
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the total number of entries across all inner maps in the readerCache.
   */
  @SuppressWarnings("unchecked")
  private int getInnerCacheSize() {
    try {
      Map<Schema, Map<Schema, ?>> cache = (Map<Schema, Map<Schema, ?>>) READER_CACHE_FIELD.get(builder);
      int total = 0;
      for (Map<Schema, ?> inner : cache.values()) {
        total += inner.size();
      }
      return total;
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the number of entries in the simple cache (used for identical reader/writer schemas).
   */
  @SuppressWarnings("unchecked")
  private int getSimpleCacheSize() {
    try {
      Map<Schema, ?> cache = (Map<Schema, ?>) READER_SIMPLE_CACHE_FIELD.get(builder);
      return cache.size();
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Trigger the reap() method in WeakIdentityHashMap by calling size().
   */
  @SuppressWarnings("unchecked")
  private void triggerReap() throws Exception {
    Map<Schema, Map<Schema, ?>> cache = (Map<Schema, Map<Schema, ?>>) READER_CACHE_FIELD.get(builder);
    cache.size(); // size() calls reap() internally
  }

  /**
   * Force garbage collection and trigger reap cycle multiple times.
   */
  private void forceGCAndReapUntil(BooleanSupplier check) throws Exception {
    for (int i = 0; i < 20 && !check.getAsBoolean(); i++) {
      System.gc();
      System.runFinalization();
      Thread.sleep(100);
      triggerReap();
    }
  }

  /**
   * Force garbage collection and trigger reap cycle multiple times.
   */
  private void forceClearSoftReferences() {
    try {
      ArrayList<Object> memory = new ArrayList<>();
      while (true) {
        memory.add(new byte[1024 * 1024 * 1024]); // Allocate 1GB blocks to fill up memory and clear soft references
      }
    } catch (OutOfMemoryError e) {
      //ignored
    }
  }
}
