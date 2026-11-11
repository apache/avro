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

package org.apache.avro.perf.test.basic;

import org.apache.avro.file.SchemaCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

public class SchemaCacheTest {

  @State(Scope.Benchmark)
  public static class TestState {
    @Param({ "5", "50", "500" })
    int schemaSize;

    @Param({ "no_cache", "concurrent", "weak" })
    String cacheType;

    SchemaCache cache;
    String schemaString;

    @Setup(Level.Trial)
    public void setup() {
      schemaString = generateSchema(schemaSize);
      switch (cacheType) {
      case "no_cache":
        cache = SchemaCache.NO_CACHE;
        break;
      case "concurrent":
        cache = SchemaCache.createConcurrentCache();
        break;
      case "weak":
        cache = SchemaCache.createWeakCache();
        break;
      default:
        throw new IllegalArgumentException("Unknown cache type: " + cacheType);
      }
    }

    private String generateSchema(int numFields) {

      StringBuilder sb = new StringBuilder();
      sb.append("{\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [");
      for (int i = 0; i < numFields; i++) {
        if (i > 0)
          sb.append(",");
        sb.append("{\"name\": \"field").append(i).append("\", \"type\": \"string\"}");
      }
      sb.append("]}");
      return sb.toString();
    }
  }

  @Benchmark
  public void getOrParseSchema(TestState state) {
    state.cache.getOrParseSchema(state.schemaString);
  }
}
