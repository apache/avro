/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import junit.framework.Assert;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestSchemaBuilder {

  private static final File DIR = new File(System.getProperty("test.dir", "/tmp"));
  private static final File FILE = new File(DIR, "test.avro");

  @Test
  public void testRecord() {
    Schema schema = SchemaBuilder
        .recordType("myrecord").namespace("org.example").aliases("oldrecord")
        .requiredString("f0")
        .requiredLong("f1").doc("This is f1")
        .optionalBoolean("f2", true)
        .build();

    Assert.assertEquals("myrecord", schema.getName());
    Assert.assertEquals("org.example", schema.getNamespace());
    Assert.assertEquals("org.example.oldrecord", schema.getAliases().iterator().next());
    Assert.assertFalse(schema.isError());
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(3, fields.size());
    Assert.assertEquals(
        new Schema.Field("f0", Schema.create(Schema.Type.STRING), null, null),
        fields.get(0));
    Assert.assertEquals(
        new Schema.Field("f1", Schema.create(Schema.Type.LONG), "This is f1", null),
        fields.get(1));

    List<Schema> types = new ArrayList<Schema>();
    types.add(Schema.create(Schema.Type.BOOLEAN));
    types.add(Schema.create(Schema.Type.NULL));
    Schema optional = Schema.createUnion(types);
    Assert.assertEquals(new Schema.Field("f2", optional, null, BooleanNode.getTrue()),
        fields.get(2));
  }

  @Test
  public void testNamespaces() {
    Schema s1 = SchemaBuilder.recordType("myrecord").namespace("org.example")
        .requiredInt("myint").build();
    Schema s2 = SchemaBuilder.recordType("org.example.myrecord")
        .requiredInt("myint").build();
    Schema s3 = SchemaBuilder.recordType("org.example.myrecord").namespace("org.example2")
        .requiredInt("myint").build();

    Assert.assertEquals("myrecord", s1.getName());
    Assert.assertEquals("myrecord", s2.getName());
    Assert.assertEquals("myrecord", s3.getName());

    Assert.assertEquals("org.example", s1.getNamespace());
    Assert.assertEquals("org.example", s2.getNamespace());
    Assert.assertEquals("org.example", s3.getNamespace()); // namespace call is ignored

    Assert.assertEquals("org.example.myrecord", s1.getFullName());
    Assert.assertEquals("org.example.myrecord", s2.getFullName());
    Assert.assertEquals("org.example.myrecord", s3.getFullName());
  }

  @Test(expected = NullPointerException.class)
  public void testMissingRecordName() {
    SchemaBuilder
        .recordType(null) // null name
        .requiredString("f0")
        .build();
  }

  @Test
  public void testError() {
    Schema schema = SchemaBuilder
        .errorType("myerror")
        .requiredString("message")
        .build();

    Assert.assertEquals("myerror", schema.getName());
    Assert.assertTrue(schema.isError());
  }

  @Test
  public void testRecordAll() throws IOException {
    GenericData.Record defaultRecord = new GenericData.Record(
        SchemaBuilder.recordType("nestedOptionalRecordWithDefault")
        .requiredBoolean("nestedRequiredBoolean").build());
    defaultRecord.put("nestedRequiredBoolean", true);

    Schema schema = SchemaBuilder.recordType("recordAll")
        .requiredBoolean("requiredBoolean")
        .requiredBoolean("requiredBooleanWithDefault").defaultValue(true) // expert
        .optionalBoolean("optionalBoolean")
        .optionalBoolean("optionalBooleanWithDefault", true)
        .requiredInt("requiredInt")
        .optionalInt("optionalInt")
        .optionalInt("optionalIntWithDefault", 1)
        .requiredLong("requiredLong")
        .optionalLong("optionalLong")
        .optionalLong("optionalLongWithDefault", 1L)
        .requiredFloat("requiredFloat")
        .optionalFloat("optionalFloat")
        .optionalFloat("optionalFloatWithDefault", 1.0f)
        .requiredDouble("requiredDouble")
        .optionalDouble("optionalDouble")
        .optionalDouble("optionalDoubleWithDefault", 1.0)
        .requiredBytes("requiredBytes")
        .optionalBytes("optionalBytes")
        .optionalBytes("optionalBytesWithDefault", new byte[]{(byte) 65})
        .requiredString("requiredString")
        .optionalString("optionalString")
        .optionalString("optionalStringWithDefault", "a")
        .requiredRecord("requiredRecord",
            SchemaBuilder.recordType("nestedRequiredRecord")
                .requiredBoolean("nestedRequiredBoolean").build())
        .optionalRecord("optionalRecord",
            SchemaBuilder.recordType("nestedOptionalRecord")
                .requiredBoolean("nestedRequiredBoolean").build())
        .optionalRecord("optionalRecordWithDefault",
            SchemaBuilder.recordType("nestedOptionalRecordWithDefault")
                .requiredBoolean("nestedRequiredBoolean").build(), defaultRecord)
        .requiredEnum("requiredEnum",
            SchemaBuilder.enumType("requiredEnum", "a", "b").build())
        .optionalEnum("optionalEnum",
            SchemaBuilder.enumType("optionalEnum", "a", "b").build())
        .optionalEnum("optionalEnumWithDefault",
            SchemaBuilder.enumType("optionalEnumWithDefault", "a", "b").build(), "b")
        .requiredArray("requiredArray",
            SchemaBuilder.arrayType(SchemaBuilder.STRING).build())
        .optionalArray("optionalArray",
            SchemaBuilder.arrayType(SchemaBuilder.STRING).build())
        .optionalArray("optionalArrayWithDefault",
            SchemaBuilder.arrayType(SchemaBuilder.STRING).build(),
            Collections.singletonList("a"))
        .requiredMap("requiredMap",
            SchemaBuilder.mapType(SchemaBuilder.STRING).build())
        .optionalMap("optionalMap",
            SchemaBuilder.mapType(SchemaBuilder.STRING).build())
        .optionalMap("optionalMapWithDefault",
            SchemaBuilder.mapType(SchemaBuilder.STRING).build(),
            Collections.singletonMap("a", "b"))
        .requiredFixed("requiredFixed",
            SchemaBuilder.fixedType("requiredFixed", 1).build())
        .optionalFixed("optionalFixed",
            SchemaBuilder.fixedType("optionalFixed", 1).build())
        .optionalFixed("optionalFixedWithDefault",
            SchemaBuilder.fixedType("optionalFixedWithDefault", 1).build(),
            new byte[]{(byte) 65})
        .unionType("unionType", SchemaBuilder.unionType(SchemaBuilder.LONG,
            SchemaBuilder.NULL).build())
        .unionBoolean("unionBooleanWithDefault", true, SchemaBuilder.INT)
        .unionInt("unionIntWithDefault", 1, SchemaBuilder.NULL)
        .unionLong("unionLongWithDefault", 1L, SchemaBuilder.INT)
        .unionFloat("unionFloatWithDefault", 1.0f, SchemaBuilder.INT)
        .unionDouble("unionDoubleWithDefault", 1.0, SchemaBuilder.INT)
        .unionBytes("unionBytesWithDefault", new byte[]{(byte) 65}, SchemaBuilder.INT)
        .unionString("unionStringWithDefault", "a", SchemaBuilder.INT)
        .unionRecord("unionRecordWithDefault",
            SchemaBuilder.recordType("nestedUnionRecordWithDefault")
                .requiredBoolean("nestedRequiredBoolean").build(), defaultRecord,
            SchemaBuilder.INT)
        .unionEnum("unionEnumWithDefault",
            SchemaBuilder.enumType("nestedUnionEnumWithDefault", "a", "b").build(), "b",
            SchemaBuilder.INT)
        .unionArray("unionArrayWithDefault",
            SchemaBuilder.arrayType(SchemaBuilder.STRING).build(),
            Collections.singletonList("a"),
            SchemaBuilder.INT)
        .unionMap("unionMapWithDefault",
            SchemaBuilder.mapType(SchemaBuilder.STRING).build(),
            Collections.singletonMap("a", "b"),
            SchemaBuilder.INT)
        .unionFixed("unionFixedWithDefault",
            SchemaBuilder.fixedType("nestedUnionFixedWithDefault", 1).build(),
            new byte[]{(byte) 65},
            SchemaBuilder.INT)
        .build();

    Schema expected = new Schema.Parser().parse
        (getClass().getResourceAsStream("/SchemaBuilder.avsc"));

    // To regenerate the expected schema, uncomment the following line
    // and copy the test output to TestSchemaBuilder.avsc
    //System.out.println(schema.toString(true));

    Assert.assertEquals(expected.toString(true), schema.toString(true));

  }

  private List<Schema.Field> fields(Schema.Field... fields) {
    return Arrays.asList(fields);
  }

  @Test
  public void testRecursiveRecord() {
    Schema schema = SchemaBuilder
        .recordType("LongList")
        .requiredLong("value")
        .optionalRecord("next", SchemaBuilder.recordReference("LongList").build())
        .build();

    Assert.assertEquals("LongList", schema.getName());
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(2, fields.size());
    Assert.assertEquals(new Schema.Field("value", Schema.create(Schema.Type.LONG), null, null),
        fields.get(0));

    Assert.assertEquals(Schema.Type.UNION, fields.get(1).schema().getType());

    Assert.assertEquals(Schema.Type.NULL, fields.get(1).schema().getTypes().get(0)
        .getType());
    Schema recordSchema = fields.get(1).schema().getTypes().get(1);
    Assert.assertEquals(Schema.Type.RECORD, recordSchema.getType());
    Assert.assertEquals("LongList", recordSchema.getName());
    Assert.assertEquals(NullNode.getInstance(), fields.get(1).defaultValue());
  }

  @Test
  public void testEnum() {
    Schema schema = SchemaBuilder.enumType("myenum", "a", "b").build();
    Assert.assertEquals(Schema.createEnum("myenum", null, null,
        Arrays.asList("a", "b")), schema);
  }

  @Test
  public void testArray() {
    Schema schema = SchemaBuilder.arrayType(SchemaBuilder.LONG).build();
    Assert.assertEquals(Schema.createArray(Schema.create(Schema.Type.LONG)), schema);
  }

  @Test
  public void testMap() {
    Schema schema = SchemaBuilder.mapType(SchemaBuilder.LONG).build();
    Assert.assertEquals(Schema.createMap(Schema.create(Schema.Type.LONG)), schema);
  }

  @Test
  public void testFixed() {
    Schema schema = SchemaBuilder.fixedType("myfixed", 16).build();
    Assert.assertEquals(Schema.createFixed("myfixed", null, null, 16), schema);
  }

  @Test
  public void testUnion() {
    Schema schema = SchemaBuilder
        .unionType(SchemaBuilder.LONG, SchemaBuilder.NULL)
        .build();
    List<Schema> types = new ArrayList<Schema>();
    types.add(Schema.create(Schema.Type.LONG));
    types.add(Schema.create(Schema.Type.NULL));
    Assert.assertEquals(Schema.createUnion(types), schema);
  }

  @Test
  public void testDefaults() throws IOException {
    Schema writeSchema = SchemaBuilder.recordType("r")
        .requiredInt("requiredInt")
        .optionalInt("optionalInt")
        .optionalInt("optionalIntWithDefault", 3)
        .build();

    GenericData.Record rec1 = new GenericRecordBuilder(writeSchema)
        .set("requiredInt", 1)
        .build();

    Assert.assertEquals(1, rec1.get("requiredInt"));
    Assert.assertEquals(null, rec1.get("optionalInt"));
    Assert.assertEquals(3, rec1.get("optionalIntWithDefault"));

    GenericData.Record rec2 = new GenericRecordBuilder(writeSchema)
        .set("requiredInt", 1)
        .set("optionalInt", 2)
        .set("optionalIntWithDefault", 13)
        .build();

    Assert.assertEquals(1, rec2.get("requiredInt"));
    Assert.assertEquals(2, rec2.get("optionalInt"));
    Assert.assertEquals(13, rec2.get("optionalIntWithDefault"));

    // write to file
    DataFileWriter<Object> writer =
        new DataFileWriter<Object>(new GenericDatumWriter<Object>());
    writer.create(writeSchema, FILE);
    writer.append(rec1);
    writer.append(rec2);
    writer.close();

    Schema readSchema = SchemaBuilder.recordType("r")
        .requiredInt("requiredInt")
        .optionalInt("optionalInt")
        .optionalInt("optionalIntWithDefault", 3)
        .optionalInt("newOptionalInt")
        .optionalInt("newOptionalIntWithDefault", 5)
        .build();

    DataFileReader<GenericData.Record> reader =
        new DataFileReader<GenericData.Record>(FILE,
            new GenericDatumReader<GenericData.Record>(writeSchema, readSchema));

    GenericData.Record rec1read = reader.iterator().next();
    Assert.assertEquals(1, rec1read.get("requiredInt"));
    Assert.assertEquals(null, rec1read.get("optionalInt"));
    Assert.assertEquals(3, rec1read.get("optionalIntWithDefault"));
    Assert.assertEquals(null, rec1read.get("newOptionalInt"));
    Assert.assertEquals(5, rec1read.get("newOptionalIntWithDefault"));

    GenericData.Record rec2read = reader.iterator().next();
    Assert.assertEquals(1, rec2read.get("requiredInt"));
    Assert.assertEquals(2, rec2read.get("optionalInt"));
    Assert.assertEquals(13, rec2read.get("optionalIntWithDefault"));
    Assert.assertEquals(null, rec2read.get("newOptionalInt"));
    Assert.assertEquals(5, rec2read.get("newOptionalIntWithDefault"));
  }

}
