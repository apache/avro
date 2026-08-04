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

package org.apache.avro.perf.test.generic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks to measure the overhead of {@code minBytesPerElement} computation
 * during array/map decoding via {@link GenericDatumReader}. Tests complex,
 * wide, and recursive schema structures to verify that the per-block schema
 * traversal cost is acceptable.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@State(Scope.Thread)
public class MinBytesPerElementTest {

  /**
   * Schema complexity levels to benchmark: - WIDE: Record with 50 fields of
   * various types - DEEP: Deeply nested records (10 levels) - RECURSIVE:
   * Self-referencing record (linked-list style)
   */
  @Param({ "WIDE", "DEEP", "RECURSIVE" })
  public String schemaType;

  private Schema arraySchema;
  private Schema mapSchema;
  private byte[] encodedArrayData;
  private byte[] encodedMapData;
  private DatumReader<GenericRecord> arrayReader;
  private DatumReader<GenericRecord> mapReader;
  private Schema arrayWrapperSchema;
  private Schema mapWrapperSchema;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Schema elementSchema = buildElementSchema(schemaType);

    // Wrap in array and map schemas for testing collection decoding paths
    arrayWrapperSchema = Schema.createRecord("ArrayWrapper", null, "test", false);
    arrayWrapperSchema.setFields(List.of(new Schema.Field("items", Schema.createArray(elementSchema), null, null)));

    mapWrapperSchema = Schema.createRecord("MapWrapper", null, "test", false);
    mapWrapperSchema.setFields(List.of(new Schema.Field("entries", Schema.createMap(elementSchema), null, null)));

    arrayReader = new GenericDatumReader<>(arrayWrapperSchema);
    mapReader = new GenericDatumReader<>(mapWrapperSchema);

    // Encode test data: array with 1000 elements
    encodedArrayData = encodeArrayData(arrayWrapperSchema, elementSchema, 1000);
    // Encode test data: map with 1000 entries
    encodedMapData = encodeMapData(mapWrapperSchema, elementSchema, 1000);
  }

  @Benchmark
  public void decodeArrayOfComplexRecords(Blackhole bh) throws IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(encodedArrayData, null);
    GenericRecord result = arrayReader.read(null, decoder);
    bh.consume(result);
  }

  @Benchmark
  public void decodeMapOfComplexRecords(Blackhole bh) throws IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(encodedMapData, null);
    GenericRecord result = mapReader.read(null, decoder);
    bh.consume(result);
  }

  private static Schema buildElementSchema(String type) {
    switch (type) {
    case "WIDE":
      return buildWideSchema();
    case "DEEP":
      return buildDeepSchema();
    case "RECURSIVE":
      return buildRecursiveSchema();
    default:
      throw new IllegalArgumentException("Unknown schema type: " + type);
    }
  }

  /**
   * Wide record: 50 fields of mixed types (int, long, double, float, string,
   * boolean, bytes, nested record).
   */
  private static Schema buildWideSchema() {
    Schema innerRecord = Schema.createRecord("Inner", null, "test", false);
    innerRecord.setFields(List.of(new Schema.Field("x", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("y", Schema.create(Schema.Type.DOUBLE), null, null)));

    List<Schema.Field> fields = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      fields.add(new Schema.Field("int_" + i, Schema.create(Schema.Type.INT), null, null));
    }
    for (int i = 0; i < 10; i++) {
      fields.add(new Schema.Field("long_" + i, Schema.create(Schema.Type.LONG), null, null));
    }
    for (int i = 0; i < 10; i++) {
      fields.add(new Schema.Field("double_" + i, Schema.create(Schema.Type.DOUBLE), null, null));
    }
    for (int i = 0; i < 10; i++) {
      fields.add(new Schema.Field("str_" + i, Schema.create(Schema.Type.STRING), null, null));
    }
    for (int i = 0; i < 5; i++) {
      fields.add(new Schema.Field("bool_" + i, Schema.create(Schema.Type.BOOLEAN), null, null));
    }
    for (int i = 0; i < 5; i++) {
      fields.add(new Schema.Field("rec_" + i, innerRecord, null, null));
    }

    Schema wide = Schema.createRecord("WideRecord", null, "test", false);
    wide.setFields(fields);
    return wide;
  }

  /**
   * Deeply nested: 10 levels of records, each containing an int and the next
   * level.
   */
  private static Schema buildDeepSchema() {
    Schema current = Schema.createRecord("Level10", null, "test", false);
    current.setFields(List.of(new Schema.Field("value", Schema.create(Schema.Type.INT), null, null)));

    for (int i = 9; i >= 1; i--) {
      Schema next = Schema.createRecord("Level" + i, null, "test", false);
      next.setFields(List.of(new Schema.Field("value", Schema.create(Schema.Type.INT), null, null),
          new Schema.Field("child", current, null, null)));
      current = next;
    }
    return current;
  }

  /**
   * Recursive: self-referencing record (linked list). The "next" field is a union
   * of null and the record itself.
   */
  private static Schema buildRecursiveSchema() {
    Schema recursive = Schema.createRecord("LinkedNode", null, "test", false);
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    Schema union = Schema.createUnion(List.of(nullSchema, recursive));
    recursive.setFields(List.of(new Schema.Field("value", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("next", union, null, null)));
    return recursive;
  }

  private static byte[] encodeArrayData(Schema wrapperSchema, Schema elementSchema, int count) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(wrapperSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

    GenericRecord wrapper = new GenericData.Record(wrapperSchema);
    List<GenericRecord> items = new ArrayList<>(count);
    Random r = new Random(42);
    for (int i = 0; i < count; i++) {
      items.add(buildRecord(elementSchema, r, 0));
    }
    wrapper.put("items", items);
    writer.write(wrapper, encoder);
    encoder.flush();
    return baos.toByteArray();
  }

  private static byte[] encodeMapData(Schema wrapperSchema, Schema elementSchema, int count) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(wrapperSchema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

    GenericRecord wrapper = new GenericData.Record(wrapperSchema);
    HashMap<String, GenericRecord> map = new HashMap<>();
    Random r = new Random(42);
    for (int i = 0; i < count; i++) {
      map.put("key_" + i, buildRecord(elementSchema, r, 0));
    }
    wrapper.put("entries", map);
    writer.write(wrapper, encoder);
    encoder.flush();
    return baos.toByteArray();
  }

  private static GenericRecord buildRecord(Schema schema, Random r, int depth) {
    GenericRecord rec = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      rec.put(field.pos(), buildValue(field.schema(), r, depth));
    }
    return rec;
  }

  private static Object buildValue(Schema schema, Random r, int depth) {
    switch (schema.getType()) {
    case INT:
      return r.nextInt();
    case LONG:
      return r.nextLong();
    case FLOAT:
      return r.nextFloat();
    case DOUBLE:
      return r.nextDouble();
    case BOOLEAN:
      return r.nextBoolean();
    case STRING:
      return "s" + r.nextInt(1000);
    case BYTES:
      byte[] b = new byte[4];
      r.nextBytes(b);
      return java.nio.ByteBuffer.wrap(b);
    case RECORD:
      return buildRecord(schema, r, depth + 1);
    case UNION:
      // For recursive schemas, limit depth
      List<Schema> types = schema.getTypes();
      if (depth > 3) {
        // Pick the null branch to terminate recursion
        for (int i = 0; i < types.size(); i++) {
          if (types.get(i).getType() == Schema.Type.NULL) {
            return null;
          }
        }
      }
      // Pick non-null branch
      for (int i = 0; i < types.size(); i++) {
        if (types.get(i).getType() != Schema.Type.NULL) {
          return buildValue(types.get(i), r, depth);
        }
      }
      return null;
    case ARRAY:
      return new ArrayList<>();
    case MAP:
      return new HashMap<>();
    case NULL:
      return null;
    default:
      return null;
    }
  }
}
