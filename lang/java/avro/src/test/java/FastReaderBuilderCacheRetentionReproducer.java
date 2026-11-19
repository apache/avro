/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * Standalone reproducer for FastReaderBuilder cache retention bug.
 *
 * Demonstrates that WeakIdentityHashMap entries in FastReaderBuilder.readerCache
 * are never evicted because RecordReader.schema holds a strong reference back to
 * the Schema weak key, preventing GC from clearing the WeakReference.
 *
 * Requirements: Apache Avro 1.12.1 (or any version with FastReaderBuilder)
 * Compile: javac -cp avro-1.12.1.jar FastReaderBuilderCacheRetentionReproducer.java
 * Run:     java -cp .:avro-1.12.1.jar FastReaderBuilderCacheRetentionReproducer
 */

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.FastReaderBuilder;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reproducer for: FastReaderBuilder WeakIdentityHashMap cache entries are never
 * GC'd.
 *
 * <h2>Problem</h2> {@link FastReaderBuilder} caches compiled
 * {@code RecordReader} objects in a
 * {@code WeakIdentityHashMap<Schema, Map<Schema, RecordReader>>}. The intent is
 * that when a Schema key becomes unreachable, the weak reference allows the
 * cache entry to be garbage-collected.
 *
 * <p>
 * However, each {@code RecordReader} is stored as a <b>value</b> in the cache
 * (via {@code computeIfAbsent}), and it holds a <b>strong reference</b> back to
 * the Schema used as the cache key (via its {@code schema} field, set in
 * {@code finishInitialization()}). The caller discarding the returned
 * {@code DatumReader} after use is irrelevant — the cache itself retains the
 * {@code RecordReader}, and the {@code RecordReader} pins the key.
 *
 * <p>
 * This is the classic <b>weak-key cache antipattern: value references key</b>.
 * The {@code WeakIdentityHashMap} can never evict entries because its own
 * values keep the keys strongly reachable:
 *
 * <pre>
 * FastReaderBuilder (application lifetime)
 *   └─ readerCache (WeakIdentityHashMap)
 *        └─ entry: weak(Schema_A) → inner map
 *             └─ entry: weak(Schema_A) → RecordReader  ← cache holds strong ref to value
 *                  └─ schema → Schema_A                 ← value holds strong ref back to key
 * </pre>
 *
 * <p>
 * For nested records, child {@code RecordReader} objects are captured in
 * {@code ExecutionStep} lambdas, creating a transitive strong-reference chain
 * that pins all levels of the schema tree.
 *
 * <h2>What this reproducer shows</h2>
 * <ol>
 * <li>Creates a 5-level nested Avro schema (100 fields total)</li>
 * <li>Calls {@code FastReaderBuilder.createDatumReader(schema)} 10 times, each
 * time with a freshly-parsed Schema (different identity, same structure)</li>
 * <li>Releases all Schema references and forces GC</li>
 * <li>Observes that the readerCache retains all entries — weak keys are never
 * cleared because the cache's own values keep them alive</li>
 * </ol>
 *
 * <h2>Expected behaviour</h2> After GC, cache entries for unreachable Schema
 * objects should be evicted.
 *
 * <h2>Actual behaviour</h2> Cache entries are never evicted. The cache is
 * self-sustaining — no external reference to the Schema is needed to keep the
 * entry alive.
 */
public class FastReaderBuilderCacheRetentionReproducer {

  private static final int NESTING_DEPTH = 5;
  private static final int FIELDS_PER_LEVEL = 20;
  private static final int NUM_SCHEMAS = 10;

  public static void main(String[] args) throws Exception {
    System.out.println("=== FastReaderBuilder Cache Retention Bug Reproducer ===\n");

    GenericData genericData = new GenericData();
    genericData.setFastReaderEnabled(true);
    FastReaderBuilder fastReaderBuilder = genericData.getFastReaderBuilder();

    Schema canonicalSchema = buildNestedSchema(0);
    String schemaJson = canonicalSchema.toString();

    System.out.println("Schema: " + NESTING_DEPTH + " nesting levels x " + FIELDS_PER_LEVEL + " fields = "
        + (NESTING_DEPTH * FIELDS_PER_LEVEL) + " total fields");
    System.out.println("Initial cache: outer=" + getOuterCacheSize(fastReaderBuilder) + " inner="
        + getInnerCacheSize(fastReaderBuilder));
    System.out.println();

    // Track schemas with WeakReferences to verify they are otherwise unreachable
    List<WeakReference<Schema>> schemaWeakRefs = new ArrayList<>();

    for (int i = 1; i <= NUM_SCHEMAS; i++) {
      // Parse a fresh Schema — different object identity each time.
      // This simulates what happens when DataFileStream parses each file's
      // header independently, or when Schema.Parser is called per-file.
      Schema freshSchema = new Schema.Parser().parse(schemaJson);
      schemaWeakRefs.add(new WeakReference<>(freshSchema));

      // Compile a DatumReader via FastReaderBuilder. This is the single-arg form
      // (writer == reader), which is what GenericDatumReader uses when no separate
      // reader schema is set. Both outer and inner cache keys are the same identity.
      DatumReader<?> reader = fastReaderBuilder.createDatumReader(freshSchema);

      // Discard the DatumReader — we don't need it. The cache entry persists.
      reader = null;

      // Release our reference to the schema.
      freshSchema = null;

      System.out.println("After schema " + i + ": outer=" + getOuterCacheSize(fastReaderBuilder) + " inner="
          + getInnerCacheSize(fastReaderBuilder));
    }

    System.out.println("\n--- Forcing GC (5 cycles with reap) ---\n");

    for (int gc = 0; gc < 5; gc++) {
      System.gc();
      Thread.sleep(200);
      triggerReap(fastReaderBuilder);
    }

    int outerSize = getOuterCacheSize(fastReaderBuilder);
    int innerSize = getInnerCacheSize(fastReaderBuilder);
    int schemasCollected = 0;
    for (WeakReference<Schema> ref : schemaWeakRefs) {
      if (ref.get() == null)
        schemasCollected++;
    }

    System.out.println("After GC:");
    System.out.println("  Outer cache entries (reader schemas): " + outerSize);
    System.out.println("  Inner cache entries (writer schemas):    " + innerSize);
    System.out
        .println("  Schema WeakRefs cleared by GC:           " + schemasCollected + " / " + schemaWeakRefs.size());
    System.out.println();
    System.out.println("  Expected if cache eviction works:");
    System.out.println("    outer=0, inner=0, schemas collected=" + NUM_SCHEMAS);
    System.out.println("  Expected with retention bug:");
    System.out
        .println("    outer=" + NUM_SCHEMAS + ", inner=" + (NUM_SCHEMAS * NESTING_DEPTH) + ", schemas collected=0");
    System.out.println();

    if (outerSize > 0 || innerSize > 0) {
      System.out.println("BUG CONFIRMED: Cache retained entries after GC.");
      System.out.println("  RecordReader.schema holds strong refs to Schema keys,");
      System.out.println("  preventing WeakIdentityHashMap from clearing entries.");
      if (schemasCollected == 0) {
        System.out.println("  Schema objects are NOT being collected (pinned by cache values).");
      }
    } else {
      System.out.println("Bug NOT reproduced — cache was correctly evicted.");
    }
  }

  /**
   * Build a nested record schema: each level has FIELDS_PER_LEVEL string fields +
   * optional child.
   */
  private static Schema buildNestedSchema(int level) {
    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("Level_" + level)
        .namespace("org.apache.avro.test.retention").fields();

    for (int f = 0; f < FIELDS_PER_LEVEL; f++) {
      fields = fields.requiredString("field_" + f);
    }
    if (level < NESTING_DEPTH - 1) {
      Schema childSchema = buildNestedSchema(level + 1);
      fields = fields.name("child").type(childSchema).noDefault();
    }
    return fields.endRecord();
  }

  /** Count entries in the outer WeakIdentityHashMap (one per reader schema). */
  @SuppressWarnings("unchecked")
  private static int getOuterCacheSize(FastReaderBuilder builder) throws Exception {
    Field field = FastReaderBuilder.class.getDeclaredField("readerCache");
    field.setAccessible(true);
    Map<Schema, Map<Schema, ?>> cache = (Map<Schema, Map<Schema, ?>>) field.get(builder);
    return cache.size();
  }

  /**
   * Count total entries across all inner maps (one per writer schema per reader
   * schema).
   */
  @SuppressWarnings("unchecked")
  private static int getInnerCacheSize(FastReaderBuilder builder) throws Exception {
    Field field = FastReaderBuilder.class.getDeclaredField("readerCache");
    field.setAccessible(true);
    Map<Schema, Map<Schema, ?>> cache = (Map<Schema, Map<Schema, ?>>) field.get(builder);
    int total = 0;
    for (Map<Schema, ?> inner : cache.values()) {
      total += inner.size();
    }
    return total;
  }

  /** Trigger WeakIdentityHashMap.reap() by calling size() on the cache. */
  @SuppressWarnings("unchecked")
  private static void triggerReap(FastReaderBuilder builder) throws Exception {
    Field field = FastReaderBuilder.class.getDeclaredField("readerCache");
    field.setAccessible(true);
    Map<Schema, Map<Schema, ?>> cache = (Map<Schema, Map<Schema, ?>>) field.get(builder);
    cache.size(); // size() calls reap() internally
  }
}
