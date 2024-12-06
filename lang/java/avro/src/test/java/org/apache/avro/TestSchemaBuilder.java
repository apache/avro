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
package org.apache.avro;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.NullNode;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSchemaBuilder {

  @TempDir
  public File DIR;

  @Test
  void record() {
    Schema schema = SchemaBuilder.record("myrecord").namespace("org.example").aliases("oldrecord").fields().name("f0")
        .aliases("f0alias").type().stringType().noDefault().name("f1").doc("This is f1").type().longType().noDefault()
        .name("f2").type().nullable().booleanType().booleanDefault(true).name("f3").type().unionOf().nullType().and()
        .booleanType().endUnion().nullDefault().endRecord();

    assertEquals("myrecord", schema.getName());
    assertEquals("org.example", schema.getNamespace());
    assertEquals("org.example.oldrecord", schema.getAliases().iterator().next());
    assertFalse(schema.isError());
    List<Schema.Field> fields = schema.getFields();
    assertEquals(4, fields.size());
    assertEquals(new Schema.Field("f0", Schema.create(Schema.Type.STRING)), fields.get(0));
    assertTrue(fields.get(0).aliases().contains("f0alias"));
    assertEquals(new Schema.Field("f1", Schema.create(Schema.Type.LONG), "This is f1"), fields.get(1));

    List<Schema> types = new ArrayList<>();
    types.add(Schema.create(Schema.Type.BOOLEAN));
    types.add(Schema.create(Schema.Type.NULL));
    Schema optional = Schema.createUnion(types);
    assertEquals(new Schema.Field("f2", optional, null, true), fields.get(2));

    List<Schema> types2 = new ArrayList<>();
    types2.add(Schema.create(Schema.Type.NULL));
    types2.add(Schema.create(Schema.Type.BOOLEAN));
    Schema optional2 = Schema.createUnion(types2);

    assertNotEquals(new Schema.Field("f3", optional2, null, (Object) null), fields.get(3));
    assertEquals(new Schema.Field("f3", optional2, null, Schema.Field.NULL_DEFAULT_VALUE), fields.get(3));
  }

  @Test
  void doc() {
    Schema s = SchemaBuilder.fixed("myfixed").doc("mydoc").size(1);
    assertEquals("mydoc", s.getDoc());
  }

  @Test
  void props() {
    Schema s = SchemaBuilder.builder().intBuilder().prop("p1", "v1").prop("p2", "v2").prop("p2", "v2real") // overwrite
        .endInt();
    int size = s.getObjectProps().size();
    assertEquals(2, size);
    assertEquals("v1", s.getProp("p1"));
    assertEquals("v2real", s.getProp("p2"));
  }

  @Test
  void objectProps() {
    Schema s = SchemaBuilder.builder().intBuilder().prop("booleanProp", true).prop("intProp", Integer.MAX_VALUE)
        .prop("longProp", Long.MAX_VALUE).prop("floatProp", 1.0f).prop("doubleProp", Double.MAX_VALUE)
        .prop("byteProp", new byte[] { 0x41, 0x42, 0x43 }).prop("stringProp", "abc").endInt();

    // object properties
    assertEquals(7, s.getObjectProps().size());
    assertTrue(s.getObjectProp("booleanProp") instanceof Boolean);
    assertEquals(true, s.getObjectProp("booleanProp"));
    assertTrue(s.getObjectProp("intProp") instanceof Integer);
    assertEquals(Integer.MAX_VALUE, s.getObjectProp("intProp"));
    assertTrue(s.getObjectProp("intProp") instanceof Integer);
    assertTrue(s.getObjectProp("longProp") instanceof Long);
    assertEquals(Long.MAX_VALUE, s.getObjectProp("longProp"));
    assertTrue(s.getObjectProp("floatProp") instanceof Float);
    // float converts to double
    assertEquals(1.0f, s.getObjectProp("floatProp"));
    assertTrue(s.getObjectProp("doubleProp") instanceof Double);
    assertEquals(Double.MAX_VALUE, s.getObjectProp("doubleProp"));
    // byte[] converts to string
    assertTrue(s.getObjectProp("byteProp") instanceof byte[]);
    assertArrayEquals(new byte[] { 0x41, 0x42, 0x43 }, (byte[]) s.getObjectProp("byteProp"));
    assertTrue(s.getObjectProp("stringProp") instanceof String);
    assertEquals("abc", s.getObjectProp("stringProp"));
  }

  @Test
  void fieldObjectProps() {
    Schema s = SchemaBuilder.builder().record("MyRecord").fields().name("myField").prop("booleanProp", true)
        .prop("intProp", Integer.MAX_VALUE).prop("longProp", Long.MAX_VALUE).prop("floatProp", 1.0f)
        .prop("doubleProp", Double.MAX_VALUE).prop("byteProp", new byte[] { 0x41, 0x42, 0x43 })
        .prop("stringProp", "abc").type().intType().noDefault().endRecord();

    Schema.Field f = s.getField("myField");

    // object properties
    assertEquals(7, f.getObjectProps().size());
    assertTrue(f.getObjectProp("booleanProp") instanceof Boolean);
    assertEquals(true, f.getObjectProp("booleanProp"));
    assertTrue(f.getObjectProp("intProp") instanceof Integer);
    assertEquals(Integer.MAX_VALUE, f.getObjectProp("intProp"));
    assertTrue(f.getObjectProp("intProp") instanceof Integer);
    assertTrue(f.getObjectProp("longProp") instanceof Long);
    assertEquals(Long.MAX_VALUE, f.getObjectProp("longProp"));
    assertTrue(f.getObjectProp("floatProp") instanceof Float);
    // float converts to double
    assertEquals(1.0f, f.getObjectProp("floatProp"));
    assertTrue(f.getObjectProp("doubleProp") instanceof Double);
    assertEquals(Double.MAX_VALUE, f.getObjectProp("doubleProp"));
    // byte[] converts to string
    assertTrue(f.getObjectProp("byteProp") instanceof byte[]);
    assertArrayEquals(new byte[] { 0x41, 0x42, 0x43 }, (byte[]) f.getObjectProp("byteProp"));
    assertTrue(f.getObjectProp("stringProp") instanceof String);
    assertEquals("abc", f.getObjectProp("stringProp"));

    assertEquals("abc", f.getObjectProp("stringProp", "default"));
    assertEquals("default", f.getObjectProp("unknwon", "default"));
  }

  @Test
  void arrayObjectProp() {
    List<Object> values = new ArrayList<>();
    values.add(true);
    values.add(Integer.MAX_VALUE);
    values.add(Long.MAX_VALUE);
    values.add(1.0f);
    values.add(Double.MAX_VALUE);
    values.add(new byte[] { 0x41, 0x42, 0x43 });
    values.add("abc");

    Schema s = SchemaBuilder.builder().intBuilder().prop("arrayProp", values).endInt();

    // object properties
    assertEquals(1, s.getObjectProps().size());

    assertTrue(s.getObjectProp("arrayProp") instanceof Collection);
    @SuppressWarnings("unchecked")
    Collection<Object> valueCollection = (Collection<Object>) s.getObjectProp("arrayProp");
    Iterator<Object> iter = valueCollection.iterator();
    assertEquals(7, valueCollection.size());
    assertEquals(true, iter.next());
    assertEquals(Integer.MAX_VALUE, iter.next());
    assertEquals(Long.MAX_VALUE, iter.next());

    assertEquals(1.0f, iter.next());
    assertEquals(Double.MAX_VALUE, iter.next());

    assertArrayEquals(new byte[] { 0x41, 0x42, 0x43 }, (byte[]) iter.next());
    assertEquals("abc", iter.next());
  }

  @Test
  void fieldArrayObjectProp() {
    List<Object> values = new ArrayList<>();
    values.add(true);
    values.add(Integer.MAX_VALUE);
    values.add(Long.MAX_VALUE);
    values.add(1.0f);
    values.add(Double.MAX_VALUE);
    values.add(new byte[] { 0x41, 0x42, 0x43 });
    values.add("abc");

    Schema s = SchemaBuilder.builder().record("MyRecord").fields().name("myField").prop("arrayProp", values).type()
        .intType().noDefault().endRecord();

    Schema.Field f = s.getField("myField");

    // object properties
    assertEquals(1, f.getObjectProps().size());

    assertTrue(f.getObjectProp("arrayProp") instanceof Collection);
    @SuppressWarnings("unchecked")
    Collection<Object> valueCollection = (Collection<Object>) f.getObjectProp("arrayProp");
    Iterator<Object> iter = valueCollection.iterator();
    assertEquals(7, valueCollection.size());
    assertEquals(true, iter.next());
    assertEquals(Integer.MAX_VALUE, iter.next());
    assertEquals(Long.MAX_VALUE, iter.next());

    assertEquals(1.0f, iter.next());
    assertEquals(Double.MAX_VALUE, iter.next());

    assertArrayEquals(new byte[] { 0x41, 0x42, 0x43 }, (byte[]) iter.next());
    assertEquals("abc", iter.next());
  }

  @Test
  void mapObjectProp() {
    Map<String, Object> values = new HashMap<>();
    values.put("booleanKey", true);
    values.put("intKey", Integer.MAX_VALUE);
    values.put("longKey", Long.MAX_VALUE);
    values.put("floatKey", 1.0f);
    values.put("doubleKey", Double.MAX_VALUE);
    values.put("byteKey", new byte[] { 0x41, 0x42, 0x43 });
    values.put("stringKey", "abc");

    Schema s = SchemaBuilder.builder().intBuilder().prop("mapProp", values).endInt();

    // object properties
    assertTrue(s.getObjectProp("mapProp") instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> valueMap = (Map<String, Object>) s.getObjectProp("mapProp");
    assertEquals(values.size(), valueMap.size());

    assertTrue(valueMap.get("booleanKey") instanceof Boolean);
    assertEquals(true, valueMap.get("booleanKey"));
    assertTrue(valueMap.get("intKey") instanceof Integer);
    assertEquals(Integer.MAX_VALUE, valueMap.get("intKey"));
    assertTrue(valueMap.get("longKey") instanceof Long);
    assertEquals(Long.MAX_VALUE, valueMap.get("longKey"));

    assertTrue(valueMap.get("floatKey") instanceof Float);
    assertEquals(1.0f, valueMap.get("floatKey"));
    assertTrue(valueMap.get("doubleKey") instanceof Double);
    assertEquals(Double.MAX_VALUE, valueMap.get("doubleKey"));

    assertTrue(valueMap.get("byteKey") instanceof byte[]);
    assertArrayEquals("ABC".getBytes(StandardCharsets.UTF_8), (byte[]) valueMap.get("byteKey"));
    assertTrue(valueMap.get("stringKey") instanceof String);
    assertEquals("abc", valueMap.get("stringKey"));
  }

  @Test
  void fieldMapObjectProp() {
    Map<String, Object> values = new HashMap<>();
    values.put("booleanKey", true);
    values.put("intKey", Integer.MAX_VALUE);
    values.put("longKey", Long.MAX_VALUE);
    values.put("floatKey", 1.0f);
    values.put("doubleKey", Double.MAX_VALUE);
    values.put("byteKey", new byte[] { 0x41, 0x42, 0x43 });
    values.put("stringKey", "abc");

    Schema s = SchemaBuilder.builder().record("MyRecord").fields().name("myField").prop("mapProp", values).type()
        .intType().noDefault().endRecord();

    Schema.Field f = s.getField("myField");

    // object properties
    assertTrue(f.getObjectProp("mapProp") instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> valueMap = (Map<String, Object>) f.getObjectProp("mapProp");
    assertEquals(values.size(), valueMap.size());

    assertTrue(valueMap.get("booleanKey") instanceof Boolean);
    assertEquals(true, valueMap.get("booleanKey"));
    assertTrue(valueMap.get("intKey") instanceof Integer);
    assertEquals(Integer.MAX_VALUE, valueMap.get("intKey"));
    assertTrue(valueMap.get("longKey") instanceof Long);
    assertEquals(Long.MAX_VALUE, valueMap.get("longKey"));

    assertTrue(valueMap.get("floatKey") instanceof Float);
    assertEquals(1.0f, valueMap.get("floatKey"));
    assertTrue(valueMap.get("doubleKey") instanceof Double);
    assertEquals(Double.MAX_VALUE, valueMap.get("doubleKey"));

    assertTrue(valueMap.get("byteKey") instanceof byte[]);
    assertEquals("ABC", new String((byte[]) valueMap.get("byteKey")));
    assertTrue(valueMap.get("stringKey") instanceof String);
    assertEquals("abc", valueMap.get("stringKey"));
  }

  @Test
  void nullObjectProp() {
    assertThrows(AvroRuntimeException.class, () -> {
      SchemaBuilder.builder().intBuilder().prop("nullProp", (Object) null).endInt();
    });
  }

  @Test
  void fieldNullObjectProp() {
    assertThrows(AvroRuntimeException.class, () -> {
      SchemaBuilder.builder().record("MyRecord").fields().name("myField").prop("nullProp", (Object) null).type()
          .intType().noDefault().endRecord();
    });
  }

  @Test
  void namespaces() {
    Schema s1 = SchemaBuilder.record("myrecord").namespace("org.example").fields().name("myint").type().intType()
        .noDefault().endRecord();
    Schema s2 = SchemaBuilder.record("org.example.myrecord").fields().name("myint").type().intType().noDefault()
        .endRecord();
    Schema s3 = SchemaBuilder.record("org.example.myrecord").namespace("org.example2").fields().name("myint").type()
        .intType().noDefault().endRecord();
    Schema s4 = SchemaBuilder.builder("org.example").record("myrecord").fields().name("myint").type().intType()
        .noDefault().endRecord();

    assertEquals("myrecord", s1.getName());
    assertEquals("myrecord", s2.getName());
    assertEquals("myrecord", s3.getName());
    assertEquals("myrecord", s4.getName());

    assertEquals("org.example", s1.getNamespace());
    assertEquals("org.example", s2.getNamespace());
    assertEquals("org.example", s3.getNamespace()); // namespace call is ignored
    assertEquals("org.example", s4.getNamespace());

    assertEquals("org.example.myrecord", s1.getFullName());
    assertEquals("org.example.myrecord", s2.getFullName());
    assertEquals("org.example.myrecord", s3.getFullName());
    assertEquals("org.example.myrecord", s4.getFullName());
  }

  @Test
  void missingRecordName() {
    assertThrows(NullPointerException.class, () -> {
      SchemaBuilder.record(null).fields() // null name
          .name("f0").type().stringType().noDefault().endRecord();
    });
  }

  @Test
  void testBoolean() {
    Schema.Type type = Schema.Type.BOOLEAN;
    Schema simple = SchemaBuilder.builder().booleanType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().booleanBuilder().prop("p", "v").endBoolean();
    assertEquals(expected, built1);
  }

  @Test
  void testInt() {
    Schema.Type type = Schema.Type.INT;
    Schema simple = SchemaBuilder.builder().intType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().intBuilder().prop("p", "v").endInt();
    assertEquals(expected, built1);
  }

  @Test
  void testLong() {
    Schema.Type type = Schema.Type.LONG;
    Schema simple = SchemaBuilder.builder().longType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().longBuilder().prop("p", "v").endLong();
    assertEquals(expected, built1);
  }

  @Test
  void testFloat() {
    Schema.Type type = Schema.Type.FLOAT;
    Schema simple = SchemaBuilder.builder().floatType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().floatBuilder().prop("p", "v").endFloat();
    assertEquals(expected, built1);
  }

  @Test
  void duble() {
    Schema.Type type = Schema.Type.DOUBLE;
    Schema simple = SchemaBuilder.builder().doubleType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().doubleBuilder().prop("p", "v").endDouble();
    assertEquals(expected, built1);
  }

  @Test
  void string() {
    Schema.Type type = Schema.Type.STRING;
    Schema simple = SchemaBuilder.builder().stringType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().stringBuilder().prop("p", "v").endString();
    assertEquals(expected, built1);
  }

  @Test
  void bytes() {
    Schema.Type type = Schema.Type.BYTES;
    Schema simple = SchemaBuilder.builder().bytesType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().bytesBuilder().prop("p", "v").endBytes();
    assertEquals(expected, built1);
  }

  @Test
  void testNull() {
    Schema.Type type = Schema.Type.NULL;
    Schema simple = SchemaBuilder.builder().nullType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder().nullBuilder().prop("p", "v").endNull();
    assertEquals(expected, built1);
  }

  private Schema primitive(Schema.Type type, Schema bare) {
    // test creation of bare schema by name
    Schema bareByName = SchemaBuilder.builder().type(type.getName());
    assertEquals(Schema.create(type), bareByName);
    assertEquals(bareByName, bare);
    // return a schema with custom prop set
    Schema p = Schema.create(type);
    p.addProp("p", "v");
    return p;
  }

//  @Test
//  public void testError() {
//    Schema schema = SchemaBuilder
//        .errorType("myerror")
//        .requiredString("message")
//        .build();
//
//    Assert.assertEquals("myerror", schema.getName());
//    Assert.assertTrue(schema.isError());
//  }

  @Test
  void recursiveRecord() {
    Schema schema = SchemaBuilder.record("LongList").fields().name("value").type().longType().noDefault().name("next")
        .type().optional().type("LongList").endRecord();

    assertEquals("LongList", schema.getName());
    List<Schema.Field> fields = schema.getFields();
    assertEquals(2, fields.size());
    assertEquals(new Schema.Field("value", Schema.create(Schema.Type.LONG), null), fields.get(0));

    assertEquals(Schema.Type.UNION, fields.get(1).schema().getType());

    assertEquals(Schema.Type.NULL, fields.get(1).schema().getTypes().get(0).getType());
    Schema recordSchema = fields.get(1).schema().getTypes().get(1);
    assertEquals(Schema.Type.RECORD, recordSchema.getType());
    assertEquals("LongList", recordSchema.getName());
    assertEquals(NullNode.getInstance(), fields.get(1).defaultValue());
  }

  @Test
  void testEnum() {
    List<String> symbols = Arrays.asList("a", "b");
    Schema expected = Schema.createEnum("myenum", null, null, symbols);
    expected.addProp("p", "v");
    Schema schema = SchemaBuilder.enumeration("myenum").prop("p", "v").symbols("a", "b");
    assertEquals(expected, schema);
  }

  @Test
  void enumWithDefault() {
    List<String> symbols = Arrays.asList("a", "b");
    String enumDefault = "a";
    Schema expected = Schema.createEnum("myenum", null, null, symbols, enumDefault);
    expected.addProp("p", "v");
    Schema schema = SchemaBuilder.enumeration("myenum").prop("p", "v").defaultSymbol(enumDefault).symbols("a", "b");
    assertEquals(expected, schema);
  }

  @Test
  void fixed() {
    Schema expected = Schema.createFixed("myfixed", null, null, 16);
    expected.addAlias("myOldFixed");
    Schema schema = SchemaBuilder.fixed("myfixed").aliases("myOldFixed").size(16);
    assertEquals(expected, schema);
  }

  @Test
  void array() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema expected = Schema.createArray(longSchema);

    Schema schema1 = SchemaBuilder.array().items().longType();
    assertEquals(expected, schema1);

    Schema schema2 = SchemaBuilder.array().items(longSchema);
    assertEquals(expected, schema2);

    Schema schema3 = SchemaBuilder.array().prop("p", "v").items().type("long");
    expected.addProp("p", "v");
    assertEquals(expected, schema3);
  }

  @Test
  void map() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema expected = Schema.createMap(intSchema);

    Schema schema1 = SchemaBuilder.map().values().intType();
    assertEquals(expected, schema1);

    Schema schema2 = SchemaBuilder.map().values(intSchema);
    assertEquals(expected, schema2);

    Schema schema3 = SchemaBuilder.map().prop("p", "v").values().type("int");
    expected.addProp("p", "v");
    assertEquals(expected, schema3);
  }

  @Test
  void unionAndNullable() {
    List<Schema> types = new ArrayList<>();
    types.add(Schema.create(Schema.Type.LONG));
    types.add(Schema.create(Schema.Type.NULL));
    Schema expected = Schema.createUnion(types);

    Schema schema = SchemaBuilder.unionOf().longType().and().nullType().endUnion();
    assertEquals(expected, schema);

    schema = SchemaBuilder.nullable().longType();
    assertEquals(expected, schema);
  }

  @Test
  void fields() {
    Schema rec = SchemaBuilder.record("Rec").fields().name("documented").doc("documented").type().nullType().noDefault()
        .name("ascending").orderAscending().type().booleanType().noDefault().name("descending").orderDescending().type()
        .floatType().noDefault().name("ignored").orderIgnore().type().doubleType().noDefault().name("aliased")
        .aliases("anAlias").type().stringType().noDefault().endRecord();
    assertEquals("documented", rec.getField("documented").doc());
    assertEquals(Order.ASCENDING, rec.getField("ascending").order());
    assertEquals(Order.DESCENDING, rec.getField("descending").order());
    assertEquals(Order.IGNORE, rec.getField("ignored").order());
    assertTrue(rec.getField("aliased").aliases().contains("anAlias"));
  }

  @Test
  void fieldShortcuts() {
    Schema full = SchemaBuilder.record("Blah").fields().name("rbool").type().booleanType().noDefault().name("obool")
        .type().optional().booleanType().name("nbool").type().nullable().booleanType().booleanDefault(true).name("rint")
        .type().intType().noDefault().name("oint").type().optional().intType().name("nint").type().nullable().intType()
        .intDefault(1).name("rlong").type().longType().noDefault().name("olong").type().optional().longType()
        .name("nlong").type().nullable().longType().longDefault(2L).name("rfloat").type().floatType().noDefault()
        .name("ofloat").type().optional().floatType().name("nfloat").type().nullable().floatType().floatDefault(-1.1f)
        .name("rdouble").type().doubleType().noDefault().name("odouble").type().optional().doubleType().name("ndouble")
        .type().nullable().doubleType().doubleDefault(99.9d).name("rstring").type().stringType().noDefault()
        .name("ostring").type().optional().stringType().name("nstring").type().nullable().stringType()
        .stringDefault("def").name("rbytes").type().bytesType().noDefault().name("obytes").type().optional().bytesType()
        .name("nbytes").type().nullable().bytesType().bytesDefault(new byte[] { 1, 2, 3 }).endRecord();

    Schema shortcut = SchemaBuilder.record("Blah").fields().requiredBoolean("rbool").optionalBoolean("obool")
        .nullableBoolean("nbool", true).requiredInt("rint").optionalInt("oint").nullableInt("nint", 1)
        .requiredLong("rlong").optionalLong("olong").nullableLong("nlong", 2L).requiredFloat("rfloat")
        .optionalFloat("ofloat").nullableFloat("nfloat", -1.1f).requiredDouble("rdouble").optionalDouble("odouble")
        .nullableDouble("ndouble", 99.9d).requiredString("rstring").optionalString("ostring")
        .nullableString("nstring", "def").requiredBytes("rbytes").optionalBytes("obytes")
        .nullableBytes("nbytes", new byte[] { 1, 2, 3 }).endRecord();

    assertEquals(full, shortcut);
  }

  @Test
  void names() {
    // no contextual namespace
    Schema r = SchemaBuilder.record("Rec").fields().name("f0").type().fixed("org.foo.MyFixed").size(1).noDefault()
        .name("f1").type("org.foo.MyFixed").noDefault().name("f2").type("org.foo.MyFixed", "").noDefault().name("f3")
        .type("org.foo.MyFixed", null).noDefault().name("f4").type("org.foo.MyFixed", "ignorethis").noDefault()
        .name("f5").type("MyFixed", "org.foo").noDefault().endRecord();
    Schema expected = Schema.createFixed("org.foo.MyFixed", null, null, 1);
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");
    checkField(r, expected, "f2");
    checkField(r, expected, "f3");
    checkField(r, expected, "f4");
    checkField(r, expected, "f5");

    // context namespace
    Schema f = SchemaBuilder.builder("").fixed("Foo").size(1);
    assertEquals(Schema.createFixed("Foo", null, null, 1), f);

    // context namespace from record matches
    r = SchemaBuilder.record("Rec").namespace("org.foo").fields().name("f0").type().fixed("MyFixed").size(1).noDefault()
        .name("f1").type("org.foo.MyFixed").noDefault().name("f2").type("org.foo.MyFixed", "").noDefault().name("f3")
        .type("org.foo.MyFixed", null).noDefault().name("f4").type("org.foo.MyFixed", "ignorethis").noDefault()
        .name("f5").type("MyFixed", "org.foo").noDefault().name("f6").type("MyFixed", null).noDefault().name("f7")
        .type("MyFixed").noDefault().endRecord();
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");
    checkField(r, expected, "f2");
    checkField(r, expected, "f3");
    checkField(r, expected, "f4");
    checkField(r, expected, "f5");
    checkField(r, expected, "f6");
    checkField(r, expected, "f7");

    // context namespace from record does not match
    r = SchemaBuilder.record("Rec").namespace("org.rec").fields().name("f0").type().fixed("MyFixed")
        .namespace("org.foo").size(1).noDefault().name("f1").type("org.foo.MyFixed").noDefault().name("f2")
        .type("org.foo.MyFixed", "").noDefault().name("f3").type("org.foo.MyFixed", null).noDefault().name("f4")
        .type("org.foo.MyFixed", "ignorethis").noDefault().name("f5").type("MyFixed", "org.foo").noDefault()
        .endRecord();
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");
    checkField(r, expected, "f2");
    checkField(r, expected, "f3");
    checkField(r, expected, "f4");
    checkField(r, expected, "f5");

    // context namespace from record, nested has no namespace
    expected = Schema.createFixed("MyFixed", null, null, 1);
    r = SchemaBuilder.record("Rec").namespace("org.rec").fields().name("f0").type().fixed("MyFixed").namespace("")
        .size(1).noDefault().name("f1").type("MyFixed", "").noDefault().endRecord();
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");

    // mimic names of primitives, but with a namesapce. This is OK
    SchemaBuilder.fixed("org.test.long").size(1);
    SchemaBuilder.fixed("long").namespace("org.test").size(1);
    SchemaBuilder.builder("org.test").fixed("long").size(1);

  }

  private void checkField(Schema r, Schema expected, String name) {
    assertEquals(expected, r.getField(name).schema());
  }

  @Test
  void namesFailRedefined() {
    assertThrows(SchemaParseException.class, () -> {
      SchemaBuilder.record("Rec").fields().name("f0").type().enumeration("MyEnum").symbols("A", "B").enumDefault("A")
          .name("f1").type().enumeration("MyEnum").symbols("X", "Y").noDefault().endRecord();
    });
  }

  @Test
  void namesFailAbsent() {
    assertThrows(SchemaParseException.class, () -> {
      SchemaBuilder.builder().type("notdefined");
    });
  }

  @Test
  void nameReserved() {
    assertThrows(AvroTypeException.class, () -> {
      SchemaBuilder.fixed("long").namespace("").size(1);
    });
  }

  @Test
  void fieldTypesAndDefaultValues() {
    byte[] bytedef = new byte[] { 3 };
    ByteBuffer bufdef = ByteBuffer.wrap(bytedef);
    String strdef = "\u0003";
    HashMap<String, String> mapdef = new HashMap<>();
    mapdef.put("a", "A");
    List<String> arrdef = Collections.singletonList("arr");

    Schema rec = SchemaBuilder.record("inner").fields().name("f").type().intType().noDefault().endRecord();

    Schema rec2 = SchemaBuilder.record("inner2").fields().name("f2").type().intType().noDefault().endRecord();

    GenericData.Record recdef = new GenericRecordBuilder(rec).set("f", 1).build();

    GenericData.Record recdef2 = new GenericRecordBuilder(rec2).set("f2", 2).build();

    Schema r = SchemaBuilder.record("r").fields().name("boolF").type().booleanType().booleanDefault(false).name("intF")
        .type().intType().intDefault(1).name("longF").type().longType().longDefault(2L).name("floatF").type()
        .floatType().floatDefault(3.0f).name("doubleF").type().doubleType().doubleDefault(4.0d).name("stringF").type()
        .stringType().stringDefault("def").name("bytesF1").type().bytesType().bytesDefault(bytedef).name("bytesF2")
        .type().bytesType().bytesDefault(bufdef).name("bytesF3").type().bytesType().bytesDefault(strdef).name("nullF")
        .type().nullType().nullDefault().name("fixedF1").type().fixed("F1").size(1).fixedDefault(bytedef)
        .name("fixedF2").type().fixed("F2").size(1).fixedDefault(bufdef).name("fixedF3").type().fixed("F3").size(1)
        .fixedDefault(strdef).name("enumF").type().enumeration("E1").symbols("S").enumDefault("S").name("mapF").type()
        .map().values().stringType().mapDefault(mapdef).name("arrayF").type().array().items().stringType()
        .arrayDefault(arrdef).name("recordF").type().record("inner").fields().name("f").type().intType().noDefault()
        .endRecord().recordDefault(recdef).name("byName").type("E1").withDefault("S")
        // union builders, one for each 'first type' in a union:
        .name("boolU").type().unionOf().booleanType().and().intType().endUnion().booleanDefault(false).name("intU")
        .type().unionOf().intType().and().longType().endUnion().intDefault(1).name("longU").type().unionOf().longType()
        .and().intType().endUnion().longDefault(2L).name("floatU").type().unionOf().floatType().and().intType()
        .endUnion().floatDefault(3.0f).name("doubleU").type().unionOf().doubleType().and().intType().endUnion()
        .doubleDefault(4.0d).name("stringU").type().unionOf().stringType().and().intType().endUnion()
        .stringDefault("def").name("bytesU").type().unionOf().bytesType().and().intType().endUnion()
        .bytesDefault(bytedef).name("nullU").type().unionOf().nullType().and().intType().endUnion().nullDefault()
        .name("fixedU").type().unionOf().fixed("F4").size(1).and().intType().endUnion().fixedDefault(bytedef)
        .name("enumU").type().unionOf().enumeration("E2").symbols("SS").and().intType().endUnion().enumDefault("SS")
        .name("mapU").type().unionOf().map().values().stringType().and().intType().endUnion().mapDefault(mapdef)
        .name("arrayU").type().unionOf().array().items().stringType().and().intType().endUnion().arrayDefault(arrdef)
        .name("recordU").type().unionOf().record("inner2").fields().name("f2").type().intType().noDefault().endRecord()
        .and().intType().endUnion().recordDefault(recdef2).endRecord();

    GenericData.Record newRec = new GenericRecordBuilder(r).build();

    assertEquals(false, newRec.get("boolF"));
    assertEquals(false, newRec.get("boolU"));
    assertEquals(1, newRec.get("intF"));
    assertEquals(1, newRec.get("intU"));
    assertEquals(2L, newRec.get("longF"));
    assertEquals(2L, newRec.get("longU"));
    assertEquals(3f, newRec.get("floatF"));
    assertEquals(3f, newRec.get("floatU"));
    assertEquals(4d, newRec.get("doubleF"));
    assertEquals(4d, newRec.get("doubleU"));
    assertEquals("def", newRec.get("stringF").toString());
    assertEquals("def", newRec.get("stringU").toString());
    assertEquals(bufdef, newRec.get("bytesF1"));
    assertEquals(bufdef, newRec.get("bytesF2"));
    assertEquals(bufdef, newRec.get("bytesF3"));
    assertEquals(bufdef, newRec.get("bytesU"));
    assertNull(newRec.get("nullF"));
    assertNull(newRec.get("nullU"));
    assertArrayEquals(bytedef, ((GenericData.Fixed) newRec.get("fixedF1")).bytes());
    assertArrayEquals(bytedef, ((GenericData.Fixed) newRec.get("fixedF2")).bytes());
    assertArrayEquals(bytedef, ((GenericData.Fixed) newRec.get("fixedF3")).bytes());
    assertArrayEquals(bytedef, ((GenericData.Fixed) newRec.get("fixedU")).bytes());
    assertEquals("S", newRec.get("enumF").toString());
    assertEquals("SS", newRec.get("enumU").toString());
    @SuppressWarnings("unchecked")
    Map<CharSequence, CharSequence> map = (Map<CharSequence, CharSequence>) newRec.get("mapF");
    assertEquals(mapdef.size(), map.size());
    for (Map.Entry<CharSequence, CharSequence> e : map.entrySet()) {
      assertEquals(mapdef.get(e.getKey().toString()), e.getValue().toString());
    }
    assertEquals(newRec.get("mapF"), newRec.get("mapU"));
    @SuppressWarnings("unchecked")
    GenericData.Array<CharSequence> arr = (GenericData.Array<CharSequence>) newRec.get("arrayF");
    assertEquals(arrdef.size(), arr.size());
    for (CharSequence c : arr) {
      assertTrue(arrdef.contains(c.toString()));
    }
    assertEquals(newRec.get("arrayF"), newRec.get("arrayU"));
    assertEquals(recdef, newRec.get("recordF"));
    assertEquals(recdef2, newRec.get("recordU"));
    assertEquals("S", newRec.get("byName").toString());
  }

  @Test
  void badDefault() {
    assertThrows(SchemaBuilderException.class, () -> {
      SchemaBuilder.record("r").fields().name("f").type(Schema.create(Schema.Type.INT)).withDefault(new Object())
          .endRecord();
    });
  }

  @Test
  void unionFieldBuild() {
    SchemaBuilder.record("r").fields().name("allUnion").type().unionOf().booleanType().and().intType().and().longType()
        .and().floatType().and().doubleType().and().stringType().and().bytesType().and().nullType().and().fixed("Fix")
        .size(1).and().enumeration("Enu").symbols("Q").and().array().items().intType().and().map().values().longType()
        .and().record("Rec").fields().name("one").type("Fix").noDefault().endRecord().endUnion().booleanDefault(false)
        .endRecord();
  }

  @Test
  void defaults() throws IOException {
    Schema writeSchema = SchemaBuilder.record("r").fields().name("requiredInt").type().intType().noDefault()
        .name("optionalInt").type().optional().intType().name("nullableIntWithDefault").type().nullable().intType()
        .intDefault(3).endRecord();

    GenericData.Record rec1 = new GenericRecordBuilder(writeSchema).set("requiredInt", 1).build();

    assertEquals(1, rec1.get("requiredInt"));
    assertNull(rec1.get("optionalInt"));
    assertEquals(3, rec1.get("nullableIntWithDefault"));

    GenericData.Record rec2 = new GenericRecordBuilder(writeSchema).set("requiredInt", 1).set("optionalInt", 2)
        .set("nullableIntWithDefault", 13).build();

    assertEquals(1, rec2.get("requiredInt"));
    assertEquals(2, rec2.get("optionalInt"));
    assertEquals(13, rec2.get("nullableIntWithDefault"));

    // write to file

    File file = new File(DIR.getPath(), "testDefaults.avro");

    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(writeSchema, file);
      writer.append(rec1);
      writer.append(rec2);
    }

    Schema readSchema = SchemaBuilder.record("r").fields().name("requiredInt").type().intType().noDefault()
        .name("optionalInt").type().optional().intType().name("nullableIntWithDefault").type().nullable().intType()
        .intDefault(3).name("newOptionalInt").type().optional().intType().name("newNullableIntWithDefault").type()
        .nullable().intType().intDefault(5).endRecord();

    try (DataFileReader<GenericData.Record> reader = new DataFileReader<>(file,
        new GenericDatumReader<>(writeSchema, readSchema))) {

      GenericData.Record rec1read = reader.iterator().next();
      assertEquals(1, rec1read.get("requiredInt"));
      assertNull(rec1read.get("optionalInt"));
      assertEquals(3, rec1read.get("nullableIntWithDefault"));
      assertNull(rec1read.get("newOptionalInt"));
      assertEquals(5, rec1read.get("newNullableIntWithDefault"));

      GenericData.Record rec2read = reader.iterator().next();
      assertEquals(1, rec2read.get("requiredInt"));
      assertEquals(2, rec2read.get("optionalInt"));
      assertEquals(13, rec2read.get("nullableIntWithDefault"));
      assertNull(rec2read.get("newOptionalInt"));
      assertEquals(5, rec2read.get("newNullableIntWithDefault"));
    }

  }

  @Test
  void defaultTypes() {
    Integer intDef = 1;
    Long longDef = 2L;
    Float floatDef = 3F;
    Double doubleDef = 4D;
    Schema schema = SchemaBuilder.record("r").fields().name("int").type().intType().intDefault(intDef).name("long")
        .type().longType().longDefault(longDef).name("float").type().floatType().floatDefault(floatDef).name("double")
        .type().doubleType().doubleDefault(doubleDef).endRecord();

    assertEquals(intDef, schema.getField("int").defaultVal(), "int field default type or value mismatch");
    assertEquals(longDef, schema.getField("long").defaultVal(), "long field default type or value mismatch");
    assertEquals(floatDef, schema.getField("float").defaultVal(), "float field default type or value mismatch");
    assertEquals(doubleDef, schema.getField("double").defaultVal(), "double field default type or value mismatch");
  }

  @Test
  void validateDefaultsEnabled() {
    assertThrows(AvroRuntimeException.class, () -> {
      try {
        SchemaBuilder.record("ValidationRecord").fields().name("IntegerField").type("int").withDefault("Invalid")
            .endRecord();
      } catch (AvroRuntimeException e) {
        assertEquals("Invalid default for field IntegerField: \"Invalid\" not a \"int\"", e.getMessage(),
            "Default behavior is to raise an exception due to record having an invalid default");
        throw e;
      }
    });
  }

  @Test
  void validateDefaultsDisabled() {
    final String fieldName = "IntegerField";
    final String defaultValue = "foo";
    Schema schema = SchemaBuilder.record("ValidationRecord").fields().name(fieldName).notValidatingDefaults()
        .type("int").withDefault(defaultValue) // Would throw an exception on endRecord() if validations enabled
        .endRecord();
    assertNull(schema.getField(fieldName).defaultVal(), "Differing types, so this returns null");
    assertEquals(defaultValue, schema.getField(fieldName).defaultValue().asText(),
        "Schema is able to be successfully created as is without validation");
  }

  /**
   * https://issues.apache.org/jira/browse/AVRO-1965
   */
  @Test
  void namespaceDefaulting() {
    Schema d = SchemaBuilder.builder().intType();
    Schema c = SchemaBuilder.record("c").fields().name("d").type(d).noDefault().endRecord();
    Schema b = SchemaBuilder.record("b").fields().name("c").type(c).noDefault().endRecord();

    Schema a1 = SchemaBuilder.record("default.a").fields().name("b").type(b).noDefault().endRecord();
    Schema a2 = new Schema.Parser().parse(a1.toString());

    assertEquals(a2, a1);
  }

  @Test
  void namesAcceptAll() throws InterruptedException {
    // Ensure that Schema.setNameValidator won't interfere with others unit tests.
    Runnable r = () -> {
      Schema.setNameValidator(NameValidator.NO_VALIDATION);
      final Schema schema = SchemaBuilder.record("7name").fields().name("123").type(Schema.create(Schema.Type.INT))
          .noDefault().endRecord();
      Assertions.assertNotNull(schema);
      Assertions.assertEquals("7name", schema.getName());
      final Schema.Field field = schema.getField("123");
      Assertions.assertEquals("123", field.name());
    };

    final Throwable[] exception = new Throwable[] { null };
    Thread t = new Thread(r);
    t.setUncaughtExceptionHandler((Thread th, Throwable e) -> exception[0] = e);
    t.start();
    t.join();
    Assertions.assertNull(exception[0], () -> exception[0].getMessage());
  }
}
