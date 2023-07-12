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
package org.apache.avro.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.compress.utils.Lists;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.protobuf.ByteString;

import org.apache.avro.protobuf.noopt.Test.Foo;
import org.apache.avro.protobuf.noopt.Test.A;
import org.apache.avro.protobuf.noopt.Test.M;
import org.apache.avro.protobuf.noopt.Test.M.N;

public class TestProtobuf {

  protected <T> GenericRecord convertProtoToAvro(T objToConvert, Class clazz) throws Exception {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<T> w = new ProtobufDatumWriter<T>(clazz);
    Schema schema = ProtobufData.get().getSchema(clazz);
    Encoder e = EncoderFactory.get().jsonEncoder(schema, bao);
    w.write(objToConvert, e);
    e.flush();
    GenericDatumReader gdr = new GenericDatumReader(schema, schema);
    Decoder d = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bao.toByteArray()));

    return (GenericRecord) gdr.read(null, d);
  }

  @Test
  void message() throws Exception {

    System.out.println(ProtobufData.get().getSchema(Foo.class).toString(true));
    Foo.Builder builder = Foo.newBuilder();
    builder.setInt32(0);
    builder.setInt64(2);
    builder.setUint32(3);
    builder.setUint64(4);
    builder.setSint32(5);
    builder.setSint64(6);
    builder.setFixed32(7);
    builder.setFixed64(8);
    builder.setSfixed32(9);
    builder.setSfixed64(10);
    builder.setFloat(1.0F);
    builder.setDouble(2.0);
    builder.setBool(true);
    builder.setString("foo");
    builder.setBytes(ByteString.copyFromUtf8("bar"));
    builder.setEnum(A.X);
    builder.addIntArray(27);
    builder.addSyms(A.Y);
    Foo fooInner = builder.build();

    Foo fooInArray = builder.build();
    builder = Foo.newBuilder(fooInArray);
    builder.addFooArray(fooInArray);

    com.google.protobuf.Timestamp ts = com.google.protobuf.Timestamp.newBuilder().setSeconds(1L).setNanos(2).build();
    builder.setTimestamp(ts);

    builder = Foo.newBuilder(fooInner);
    builder.setFoo(fooInner);
    Foo foo = builder.build();

    System.out.println(foo);

    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<Foo> w = new ProtobufDatumWriter<>(Foo.class);
    Encoder e = EncoderFactory.get().binaryEncoder(bao, null);
    w.write(foo, e);
    e.flush();

    Object o = new ProtobufDatumReader<>(Foo.class).read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null));

    assertEquals(foo, o);
  }

  @Test
  void messageWithEmptyArray() throws Exception {
    Foo foo = Foo.newBuilder().setInt32(5).setBool(true).build();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<Foo> w = new ProtobufDatumWriter<>(Foo.class);
    Encoder e = EncoderFactory.get().binaryEncoder(bao, null);
    w.write(foo, e);
    e.flush();
    Foo o = new ProtobufDatumReader<>(Foo.class).read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null));

    assertEquals(foo.getInt32(), o.getInt32());
    assertEquals(foo.getBool(), o.getBool());
    assertEquals(0, o.getFooArrayCount());
  }

  @Test
  void emptyArray() throws Exception {
    Schema s = ProtobufData.get().getSchema(Foo.class);
    assertEquals(s.getField("fooArray").defaultVal(), Lists.newArrayList());
  }

  @Test
  void nestedEnum() throws Exception {
    Schema s = ProtobufData.get().getSchema(N.class);
    assertEquals(N.class.getName(), SpecificData.get().getClass(s).getName());
  }

  @Test
  void nestedClassNamespace() throws Exception {
    Schema s = ProtobufData.get().getSchema(Foo.class);
    assertEquals(org.apache.avro.protobuf.noopt.Test.class.getName(), s.getNamespace());
  }

  @Test
  void classNamespaceInMultipleFiles() throws Exception {
    Schema fooSchema = ProtobufData.get().getSchema(org.apache.avro.protobuf.multiplefiles.Foo.class);
    assertEquals(org.apache.avro.protobuf.multiplefiles.Foo.class.getPackage().getName(), fooSchema.getNamespace());

    Schema nSchema = ProtobufData.get().getSchema(org.apache.avro.protobuf.multiplefiles.M.N.class);
    assertEquals(org.apache.avro.protobuf.multiplefiles.M.class.getName(), nSchema.getNamespace());
  }

  @Test
  void getNonRepeatedSchemaWithLogicalType() throws Exception {
    ProtoConversions.TimestampMillisConversion conversion = new ProtoConversions.TimestampMillisConversion();

    // Don't convert to logical type if conversion isn't set
    ProtobufData instance1 = new ProtobufData();
    Schema s1 = instance1.getSchema(com.google.protobuf.Timestamp.class);
    assertNotEquals(conversion.getRecommendedSchema(), s1);

    // Convert to logical type if conversion is set
    ProtobufData instance2 = new ProtobufData();
    instance2.addLogicalTypeConversion(conversion);
    Schema s2 = instance2.getSchema(com.google.protobuf.Timestamp.class);
    assertEquals(conversion.getRecommendedSchema(), s2);
  }

  @Test
  void nestedEnumWithValue() throws Exception {
    Schema enumSchema = Schema.createEnum("N", null, null, Arrays.asList("A"));
    GenericEnumSymbol enumA = new GenericData.EnumSymbol(enumSchema, "A");

    M.Builder builder = M.newBuilder();
    builder.setEnumN(M.N.A);

    GenericRecord converted = convertProtoToAvro(builder.build(), M.class);

    assertEquals(0, ((GenericEnumSymbol) converted.get("enumN")).compareTo(enumA));
  }

  @Test
  void nestedEnumWithNull() throws Exception {
    M.Builder builder = M.newBuilder();

    GenericRecord converted = convertProtoToAvro(builder.build(), M.class);

    assertNull(converted.get("enumN"));
  }

  @Test
  void handlingOptionalValuesCorrectly() throws Exception {
    Schema enumSchema = Schema.createEnum("A", null, null, Arrays.asList("X", "Y", "Z"));
    GenericEnumSymbol enumZ = new GenericData.EnumSymbol(enumSchema, "Z");

    Foo.Builder builder = Foo.newBuilder();
    builder.setInt32(10);
    builder.setInt64(2);
    Foo foo = builder.build();

    GenericRecord converted = convertProtoToAvro(foo, Foo.class);

    assertEquals(10, converted.get("int32"));
    assertEquals(2L, converted.get("int64"));
    assertNull(converted.get("uint32"));
    assertNull(converted.get("uint64"));
    assertNull(converted.get("sint32"));
    assertNull(converted.get("sint64"));
    assertNull(converted.get("fixed32"));
    assertNull(converted.get("fixed64"));
    assertNull(converted.get("sfixed32"));
    assertNull(converted.get("sfixed64"));
    assertNull(converted.get("float"));
    assertNull(converted.get("double"));
    assertNull(converted.get("bool"));
    assertNull(converted.get("string"));
    assertNull(converted.get("bytes"));
    assertEquals(0, ((GenericEnumSymbol) converted.get("enum")).compareTo(enumZ));
    assertEquals(0, ((GenericData.Array) converted.get("intArray")).size());
    assertEquals(0, ((GenericData.Array) converted.get("fooArray")).size());
    assertEquals(0, ((GenericData.Array) converted.get("syms")).size());
  }
}
