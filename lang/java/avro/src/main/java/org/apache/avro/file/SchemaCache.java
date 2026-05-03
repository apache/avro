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

import org.apache.avro.NameValidator;
import org.apache.avro.Schema;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstract base class for caching parsed Avro schemas. Provides different
 * caching strategies including no cache, concurrent cache, and weak cache.
 */
public abstract class SchemaCache {

  /**
   * A cache implementation that does not cache schemas, always parsing them.
   */
  public static final SchemaCache NO_CACHE = new SchemaCache() {
    @Override
    public Schema getOrParseSchema(String metaString) {
      return parse(metaString);
    }
  };

  /**
   * Gets the schema for the given meta string, parsing it if not known.
   *
   * @param metaString the schema string to parse or retrieve from cache
   * @return the parsed Schema
   */
  public abstract Schema getOrParseSchema(String metaString);

  /** return the number of cached entries */
  public int size() {
    return 0;
  }

  /**
   * Parses the schema string into a Schema object.
   *
   * @param metaString the schema string
   * @return the parsed Schema
   */
  protected Schema parse(String metaString) {
    return new Schema.Parser(NameValidator.NO_VALIDATION).setValidateDefaults(false).parse(metaString);
  }

  /**
   * Creates a concurrent cache that strongly references schemas.
   *
   * @return a SchemaCache with concurrent strong caching
   */
  public static SchemaCache createConcurrentCache() {
    return new SchemaCache() {
      private final ConcurrentMap<String, Schema> cache = new ConcurrentHashMap<>();

      @Override
      public int size() {
        return cache.size();
      }

      @Override
      public Schema getOrParseSchema(String metaString) {
        return cache.computeIfAbsent(metaString, this::parse);
      }
    };
  }

  /**
   * A cache implementation that uses weak references for schema values, allowing
   * them to be garbage collected when not strongly referenced.
   */
  public static class WeakSchemaCache extends SchemaCache {
    /**
     * A weak cache implementation that allows schema values to be garbage
     * collected.
     */
    public static final SchemaCache INSTANCE = createWeakCache();

    private final ConcurrentMap<String, WeakValueRef> cache = new ConcurrentHashMap<>();
    private final ReferenceQueue<Schema> queue = new ReferenceQueue<>();

    @Override
    public Schema getOrParseSchema(String metaString) {
      trim();

      return cache.compute(metaString, (k, ref) -> {
        if (ref != null) {
          Schema schema = ref.get();
          if (schema != null) {
            return ref;
          }
        }
        // Absent or cleared, parse new
        Schema schema = parse(metaString);
        return new WeakValueRef(k, schema, queue);
      }).get();
    }

    @Override
    public int size() {
      trim();
      return cache.size();
    }

    /**
     * Cleans up entries with cleared weak references from the cache.
     */
    void trim() {
      // Clean up cleared references
      WeakValueRef clearedRef;
      while ((clearedRef = (WeakValueRef) queue.poll()) != null) {
        cache.remove(clearedRef.key, clearedRef);
      }
    }

    private static class WeakValueRef extends WeakReference<Schema> {
      final String key;

      WeakValueRef(String key, Schema referent, ReferenceQueue<Schema> q) {
        super(referent, q);
        this.key = key;
      }
    }
  }

  /**
   * Creates a weak cache that allows schema values to be garbage collected.
   *
   * @return a WeakSchemaCache instance
   */
  public static WeakSchemaCache createWeakCache() {
    return new WeakSchemaCache();
  }
}
