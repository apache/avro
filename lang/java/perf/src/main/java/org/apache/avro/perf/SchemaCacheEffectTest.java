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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Benchmark)
public class SchemaCacheEffectTest {

  @Param({ "5", "50", "500" })
  private int numFields;

  @Param({ "none", "weak", "soft" })
  private String cacheType;

  @Param({ "1", "10", "100" })
  private int numRecords;

  private String schemaString;
  private Schema schema;
  private byte[] avroData;

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
    schema = new Schema.Parser().parse(schemaString);

    // Set system property for cache type
    System.setProperty("avro.schema.cache", cacheType);

    // Create Avro data with specified number of records
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      writer.create(schema, baos);
      for (int i = 0; i < numRecords; i++) {
        GenericRecord record = new GenericData.Record(schema);
        for (int j = 0; j < numFields; j++) {
          record.put("f" + j, "value" + i + "_" + j);
        }
        writer.append(record);
      }
      writer.close();
      avroData = baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Benchmark
  @Threads(5)
  public void benchmarkDataReading(Blackhole bh) throws IOException {
    try (DataFileStream<GenericRecord> reader = new DataFileStream<>(new ByteArrayInputStream(avroData),
        new GenericDatumReader<GenericRecord>())) {
      for (GenericRecord record : reader) {
        bh.consume(record);
        // Consume the record (do nothing for benchmark)
      }
    }
  }

}
