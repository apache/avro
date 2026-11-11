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

package org.apache.avro.perf;

import org.apache.avro.Schema;
import org.apache.avro.file.SchemaCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Benchmark)
public class SchemaCacheTest {

  @Param({ "5", "50", "500" })
  private int numFields;

  @Param({ "NO_CACHE", "CONCURRENT", "WEAK" })
  private String cacheType;

  private SchemaCache cache;
  private String schemaString;

  @Setup(Level.Trial)
  public void setup() {
    // Create schema string with specified number of fields
    StringBuilder sb = new StringBuilder();
    sb.append("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [");
    for (int i = 0; i < numFields; i++) {
      if (i > 0)
        sb.append(",");
      sb.append("{\"name\": \"f").append(i).append("\", \"type\": \"string\"}");
    }
    sb.append("]}");
    schemaString = sb.toString();

    // Initialize cache based on type
    switch (cacheType) {
    case "NO_CACHE":
      cache = SchemaCache.NO_CACHE;
      break;
    case "CONCURRENT":
      cache = SchemaCache.createConcurrentCache();
      break;
    case "WEAK":
      cache = SchemaCache.createWeakCache();
      break;
    default:
      throw new IllegalArgumentException("Unknown cache type: " + cacheType);
    }

  }

  @Benchmark
  public Schema benchmarkSchemaParsing() {
    return cache.getOrParseSchema(schemaString);
  }

}
