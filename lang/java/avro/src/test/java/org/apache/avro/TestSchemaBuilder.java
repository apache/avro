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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field.Order;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.node.NullNode;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaBuilder {

  private static final File DIR = new File(System.getProperty("test.dir", "/tmp"));
  private static final File FILE = new File(DIR, "test.avro");

  @Test
  public void testRecord() {
    Schema schema = SchemaBuilder
        .record("myrecord").namespace("org.example").aliases("oldrecord").fields()
        .name("f0").aliases("f0alias").type().stringType().noDefault()
        .name("f1").doc("This is f1").type().longType().noDefault()
        .name("f2").type().nullable().booleanType().booleanDefault(true)
        .endRecord();

    Assert.assertEquals("myrecord", schema.getName());
    Assert.assertEquals("org.example", schema.getNamespace());
    Assert.assertEquals("org.example.oldrecord", schema.getAliases().iterator().next());
    Assert.assertFalse(schema.isError());
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(3, fields.size());
    Assert.assertEquals(
        new Schema.Field("f0", Schema.create(Schema.Type.STRING), null, null),
        fields.get(0));
    Assert.assertTrue(fields.get(0).aliases().contains("f0alias"));
    Assert.assertEquals(
        new Schema.Field("f1", Schema.create(Schema.Type.LONG), "This is f1", null),
        fields.get(1));

    List<Schema> types = new ArrayList<Schema>();
    types.add(Schema.create(Schema.Type.BOOLEAN));
    types.add(Schema.create(Schema.Type.NULL));
    Schema optional = Schema.createUnion(types);
    Assert.assertEquals(new Schema.Field("f2", optional, null, true),
        fields.get(2));
  }

  @Test
  public void testDoc() {
    Schema s = SchemaBuilder.fixed("myfixed").doc("mydoc").size(1);
    Assert.assertEquals("mydoc", s.getDoc());
  }

  @Test
  public void testProps() {
    Schema s = SchemaBuilder.builder().intBuilder()
      .prop("p1", "v1")
      .prop("p2", "v2")
      .prop("p2", "v2real") // overwrite
      .endInt();
    @SuppressWarnings("deprecation")
    int size = s.getProps().size();
    Assert.assertEquals(2, size);
    Assert.assertEquals("v1", s.getProp("p1"));
    Assert.assertEquals("v2real", s.getProp("p2"));
  }

  @Test
  public void testNamespaces() {
    Schema s1 = SchemaBuilder.record("myrecord")
      .namespace("org.example")
      .fields()
        .name("myint").type().intType().noDefault()
        .endRecord();
    Schema s2 = SchemaBuilder.record("org.example.myrecord")
      .fields()
        .name("myint").type().intType().noDefault()
        .endRecord();
    Schema s3 = SchemaBuilder.record("org.example.myrecord")
      .namespace("org.example2")
      .fields()
        .name("myint").type().intType().noDefault()
        .endRecord();
    Schema s4 = SchemaBuilder.builder("org.example").record("myrecord")
      .fields()
        .name("myint").type().intType().noDefault()
        .endRecord();

    Assert.assertEquals("myrecord", s1.getName());
    Assert.assertEquals("myrecord", s2.getName());
    Assert.assertEquals("myrecord", s3.getName());
    Assert.assertEquals("myrecord", s4.getName());

    Assert.assertEquals("org.example", s1.getNamespace());
    Assert.assertEquals("org.example", s2.getNamespace());
    Assert.assertEquals("org.example", s3.getNamespace()); // namespace call is ignored
    Assert.assertEquals("org.example", s4.getNamespace());

    Assert.assertEquals("org.example.myrecord", s1.getFullName());
    Assert.assertEquals("org.example.myrecord", s2.getFullName());
    Assert.assertEquals("org.example.myrecord", s3.getFullName());
    Assert.assertEquals("org.example.myrecord", s4.getFullName());
  }

  @Test(expected = NullPointerException.class)
  public void testMissingRecordName() {
    SchemaBuilder
      .record(null).fields() // null name
        .name("f0").type().stringType().noDefault()
        .endRecord();
  }

  @Test
  public void testBoolean() {
    Schema.Type type = Schema.Type.BOOLEAN;
    Schema simple = SchemaBuilder.builder().booleanType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .booleanBuilder().prop("p", "v").endBoolean();
    Assert.assertEquals(expected, built1);
  }

  @Test
  public void testInt() {
    Schema.Type type = Schema.Type.INT;
    Schema simple = SchemaBuilder.builder().intType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .intBuilder().prop("p", "v").endInt();
    Assert.assertEquals(expected, built1);
  }

  @Test
  public void testLong() {
    Schema.Type type = Schema.Type.LONG;
    Schema simple = SchemaBuilder.builder().longType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .longBuilder().prop("p", "v").endLong();
    Assert.assertEquals(expected, built1);
  }

  @Test
  public void testFloat() {
    Schema.Type type = Schema.Type.FLOAT;
    Schema simple = SchemaBuilder.builder().floatType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .floatBuilder().prop("p", "v").endFloat();
    Assert.assertEquals(expected, built1);
  }

  @Test
  public void testDuble() {
    Schema.Type type = Schema.Type.DOUBLE;
    Schema simple = SchemaBuilder.builder().doubleType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .doubleBuilder().prop("p", "v").endDouble();
    Assert.assertEquals(expected, built1);
  }

  @Test
  public void testString() {
    Schema.Type type = Schema.Type.STRING;
    Schema simple = SchemaBuilder.builder().stringType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .stringBuilder().prop("p", "v").endString();
    Assert.assertEquals(expected, built1);
  }

  @Test
  public void testBytes() {
    Schema.Type type = Schema.Type.BYTES;
    Schema simple = SchemaBuilder.builder().bytesType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .bytesBuilder().prop("p", "v").endBytes();
    Assert.assertEquals(expected, built1);
  }

  @Test
  public void testNull() {
    Schema.Type type = Schema.Type.NULL;
    Schema simple = SchemaBuilder.builder().nullType();
    Schema expected = primitive(type, simple);
    Schema built1 = SchemaBuilder.builder()
        .nullBuilder().prop("p", "v").endNull();
    Assert.assertEquals(expected, built1);
  }


  private Schema primitive(Schema.Type type, Schema bare) {
    // test creation of bare schema by name
    Schema bareByName = SchemaBuilder.builder().type(type.getName());
    Assert.assertEquals(Schema.create(type), bareByName);
    Assert.assertEquals(bareByName, bare);
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
  public void testRecursiveRecord() {
    Schema schema = SchemaBuilder.record("LongList").fields()
      .name("value").type().longType().noDefault()
      .name("next").type().optional().type("LongList")
      .endRecord();

    Assert.assertEquals("LongList", schema.getName());
    List<Schema.Field> fields = schema.getFields();
    Assert.assertEquals(2, fields.size());
    Assert.assertEquals(
        new Schema.Field("value", Schema.create(Schema.Type.LONG), null, null),
        fields.get(0));

    Assert.assertEquals(
        Schema.Type.UNION,
        fields.get(1).schema().getType());

    Assert.assertEquals(
        Schema.Type.NULL,
        fields.get(1).schema().getTypes().get(0).getType());
    Schema recordSchema = fields.get(1).schema().getTypes().get(1);
    Assert.assertEquals(Schema.Type.RECORD, recordSchema.getType());
    Assert.assertEquals("LongList", recordSchema.getName());
    Assert.assertEquals(NullNode.getInstance(), fields.get(1).defaultValue());
  }

  @Test
  public void testEnum() {
    List<String> symbols = Arrays.asList("a", "b");
    Schema expected = Schema.createEnum("myenum", null, null, symbols);
    expected.addProp("p", "v");
    Schema schema = SchemaBuilder.enumeration("myenum")
        .prop("p", "v").symbols("a", "b");
    Assert.assertEquals(expected, schema);
  }

  @Test
  public void testFixed() {
    Schema expected = Schema.createFixed("myfixed", null, null, 16);
    expected.addAlias("myOldFixed");
    Schema schema = SchemaBuilder.fixed("myfixed")
        .aliases("myOldFixed").size(16);
    Assert.assertEquals(expected, schema);
  }

  @Test
  public void testArray() {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema expected = Schema.createArray(longSchema);

    Schema schema1 = SchemaBuilder.array().items().longType();
    Assert.assertEquals(expected, schema1);

    Schema schema2 = SchemaBuilder.array().items(longSchema);
    Assert.assertEquals(expected, schema2);

    Schema schema3 = SchemaBuilder.array().prop("p", "v")
        .items().type("long");
    expected.addProp("p", "v");
    Assert.assertEquals(expected, schema3);
}

  @Test
  public void testMap() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema expected = Schema.createMap(intSchema);

    Schema schema1 = SchemaBuilder.map().values().intType();
    Assert.assertEquals(expected, schema1);

    Schema schema2 = SchemaBuilder.map().values(intSchema);
    Assert.assertEquals(expected, schema2);

    Schema schema3 = SchemaBuilder.map().prop("p", "v")
        .values().type("int");
    expected.addProp("p", "v");
    Assert.assertEquals(expected, schema3);
  }

  @Test
  public void testUnionAndNullable() {
    List<Schema> types = new ArrayList<Schema>();
    types.add(Schema.create(Schema.Type.LONG));
    types.add(Schema.create(Schema.Type.NULL));
    Schema expected = Schema.createUnion(types);

    Schema schema = SchemaBuilder.unionOf()
        .longType().and()
        .nullType().endUnion();
    Assert.assertEquals(expected, schema);

    schema = SchemaBuilder.nullable().longType();
    Assert.assertEquals(expected, schema);
  }

  @Test
  public void testFields() {
    Schema rec = SchemaBuilder.record("Rec").fields()
      .name("documented").doc("documented").type().nullType().noDefault()
      .name("ascending").orderAscending().type().booleanType().noDefault()
      .name("descending").orderDescending().type().floatType().noDefault()
      .name("ignored").orderIgnore().type().doubleType().noDefault()
      .name("aliased").aliases("anAlias").type().stringType().noDefault()
      .endRecord();
    Assert.assertEquals("documented", rec.getField("documented").doc());
    Assert.assertEquals(Order.ASCENDING, rec.getField("ascending").order());
    Assert.assertEquals(Order.DESCENDING, rec.getField("descending").order());
    Assert.assertEquals(Order.IGNORE, rec.getField("ignored").order());
    Assert.assertTrue(rec.getField("aliased").aliases().contains("anAlias"));
  }

  @Test
  public void testFieldShortcuts() {
    Schema full = SchemaBuilder.record("Blah").fields()
        .name("rbool").type().booleanType().noDefault()
        .name("obool").type().optional().booleanType()
        .name("nbool").type().nullable().booleanType().booleanDefault(true)
        .name("rint").type().intType().noDefault()
        .name("oint").type().optional().intType()
        .name("nint").type().nullable().intType().intDefault(1)
        .name("rlong").type().longType().noDefault()
        .name("olong").type().optional().longType()
        .name("nlong").type().nullable().longType().longDefault(2L)
        .name("rfloat").type().floatType().noDefault()
        .name("ofloat").type().optional().floatType()
        .name("nfloat").type().nullable().floatType().floatDefault(-1.1f)
        .name("rdouble").type().doubleType().noDefault()
        .name("odouble").type().optional().doubleType()
        .name("ndouble").type().nullable().doubleType().doubleDefault(99.9d)
        .name("rstring").type().stringType().noDefault()
        .name("ostring").type().optional().stringType()
        .name("nstring").type().nullable().stringType().stringDefault("def")
        .name("rbytes").type().bytesType().noDefault()
        .name("obytes").type().optional().bytesType()
        .name("nbytes").type().nullable().bytesType().bytesDefault(new byte[] {1,2,3})
        .endRecord();

    Schema shortcut = SchemaBuilder.record("Blah").fields()
        .requiredBoolean("rbool")
        .optionalBoolean("obool")
        .nullableBoolean("nbool", true)
        .requiredInt("rint")
        .optionalInt("oint")
        .nullableInt("nint", 1)
        .requiredLong("rlong")
        .optionalLong("olong")
        .nullableLong("nlong", 2L)
        .requiredFloat("rfloat")
        .optionalFloat("ofloat")
        .nullableFloat("nfloat", -1.1f)
        .requiredDouble("rdouble")
        .optionalDouble("odouble")
        .nullableDouble("ndouble", 99.9d)
        .requiredString("rstring")
        .optionalString("ostring")
        .nullableString("nstring", "def")
        .requiredBytes("rbytes")
        .optionalBytes("obytes")
        .nullableBytes("nbytes", new byte[] {1,2,3})
        .endRecord();

    Assert.assertEquals(full, shortcut);
  }

  @Test
  public void testNames() {
    // no contextual namespace
    Schema r = SchemaBuilder.record("Rec").fields()
      .name("f0").type().fixed("org.foo.MyFixed").size(1).noDefault()
      .name("f1").type("org.foo.MyFixed").noDefault()
      .name("f2").type("org.foo.MyFixed", "").noDefault()
      .name("f3").type("org.foo.MyFixed", null).noDefault()
      .name("f4").type("org.foo.MyFixed", "ignorethis").noDefault()
      .name("f5").type("MyFixed", "org.foo").noDefault()
      .endRecord();
    Schema expected = Schema.createFixed("org.foo.MyFixed", null, null, 1);
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");
    checkField(r, expected, "f2");
    checkField(r, expected, "f3");
    checkField(r, expected, "f4");
    checkField(r, expected, "f5");

    // context namespace
    Schema f = SchemaBuilder.builder("").fixed("Foo").size(1);
    Assert.assertEquals(Schema.createFixed("Foo", null, null, 1), f);

    // context namespace from record matches
    r = SchemaBuilder.record("Rec").namespace("org.foo").fields()
        .name("f0").type().fixed("MyFixed").size(1).noDefault()
        .name("f1").type("org.foo.MyFixed").noDefault()
        .name("f2").type("org.foo.MyFixed", "").noDefault()
        .name("f3").type("org.foo.MyFixed", null).noDefault()
        .name("f4").type("org.foo.MyFixed", "ignorethis").noDefault()
        .name("f5").type("MyFixed", "org.foo").noDefault()
        .name("f6").type("MyFixed", null).noDefault()
        .name("f7").type("MyFixed").noDefault()
        .endRecord();
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");
    checkField(r, expected, "f2");
    checkField(r, expected, "f3");
    checkField(r, expected, "f4");
    checkField(r, expected, "f5");
    checkField(r, expected, "f6");
    checkField(r, expected, "f7");

    // context namespace from record does not match
    r = SchemaBuilder.record("Rec").namespace("org.rec").fields()
        .name("f0").type().fixed("MyFixed").namespace("org.foo").size(1).noDefault()
        .name("f1").type("org.foo.MyFixed").noDefault()
        .name("f2").type("org.foo.MyFixed", "").noDefault()
        .name("f3").type("org.foo.MyFixed", null).noDefault()
        .name("f4").type("org.foo.MyFixed", "ignorethis").noDefault()
        .name("f5").type("MyFixed", "org.foo").noDefault()
        .endRecord();
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");
    checkField(r, expected, "f2");
    checkField(r, expected, "f3");
    checkField(r, expected, "f4");
    checkField(r, expected, "f5");

    // context namespace from record, nested has no namespace
    expected = Schema.createFixed("MyFixed", null, null, 1);
    r = SchemaBuilder.record("Rec").namespace("org.rec").fields()
        .name("f0").type().fixed("MyFixed").namespace("").size(1).noDefault()
        .name("f1").type("MyFixed", "").noDefault()
        .endRecord();
    checkField(r, expected, "f0");
    checkField(r, expected, "f1");

    // mimic names of primitives, but with a namesapce.  This is OK
    SchemaBuilder.fixed("org.test.long").size(1);
    SchemaBuilder.fixed("long").namespace("org.test").size(1);
    SchemaBuilder.builder("org.test").fixed("long").size(1);

  }

  private void checkField(Schema r, Schema expected, String name) {
    Assert.assertEquals(expected, r.getField(name).schema());
  }

  @Test(expected=SchemaParseException.class)
  public void testNamesFailRedefined() {
    SchemaBuilder.record("Rec").fields()
      .name("f0").type().enumeration("MyEnum").symbols("A", "B").enumDefault("A")
      .name("f1").type().enumeration("MyEnum").symbols("X", "Y").noDefault()
      .endRecord();
  }

  @Test(expected=SchemaParseException.class)
  public void testNamesFailAbsent() {
    SchemaBuilder.builder().type("notdefined");
  }

  @Test(expected=AvroTypeException.class)
  public void testNameReserved() {
    SchemaBuilder.fixed("long").namespace("").size(1);
  }

  @Test
  public void testFieldTypesAndDefaultValues() {
    byte[] bytedef = new byte[]{3};
    ByteBuffer bufdef = ByteBuffer.wrap(bytedef);
    String strdef = "\u0003";
    HashMap<String, String> mapdef = new HashMap<String, String>();
    mapdef.put("a", "A");
    ArrayList<String> arrdef = new ArrayList<String>();
    arrdef.add("arr");

    Schema rec = SchemaBuilder.record("inner").fields()
      .name("f").type().intType().noDefault()
      .endRecord();

    Schema rec2 = SchemaBuilder.record("inner2").fields()
      .name("f2").type().intType().noDefault()
      .endRecord();

    GenericData.Record recdef =
        new GenericRecordBuilder(rec).set("f", 1).build();

    GenericData.Record recdef2 =
        new GenericRecordBuilder(rec2).set("f2", 2).build();

    Schema r = SchemaBuilder.record("r").fields()
      .name("boolF").type().booleanType().booleanDefault(false)
      .name("intF").type().intType().intDefault(1)
      .name("longF").type().longType().longDefault(2L)
      .name("floatF").type().floatType().floatDefault(3.0f)
      .name("doubleF").type().doubleType().doubleDefault(4.0d)
      .name("stringF").type().stringType().stringDefault("def")
      .name("bytesF1").type().bytesType().bytesDefault(bytedef)
      .name("bytesF2").type().bytesType().bytesDefault(bufdef)
      .name("bytesF3").type().bytesType().bytesDefault(strdef)
      .name("nullF").type().nullType().nullDefault()
      .name("fixedF1").type().fixed("F1").size(1).fixedDefault(bytedef)
      .name("fixedF2").type().fixed("F2").size(1).fixedDefault(bufdef)
      .name("fixedF3").type().fixed("F3").size(1).fixedDefault(strdef)
      .name("enumF").type().enumeration("E1").symbols("S").enumDefault("S")
      .name("mapF").type().map().values().stringType()
        .mapDefault(mapdef)
      .name("arrayF").type().array().items().stringType()
        .arrayDefault(arrdef)
      .name("recordF").type().record("inner").fields()
        .name("f").type().intType().noDefault()
        .endRecord().recordDefault(recdef)
      .name("byName").type("E1").withDefault("S")
      // union builders, one for each 'first type' in a union:
      .name("boolU").type().unionOf().booleanType().and()
        .intType().endUnion().booleanDefault(false)
      .name("intU").type().unionOf().intType().and()
        .longType().endUnion().intDefault(1)
      .name("longU").type().unionOf().longType().and()
        .intType().endUnion().longDefault(2L)
      .name("floatU").type().unionOf().floatType().and()
        .intType().endUnion().floatDefault(3.0f)
      .name("doubleU").type().unionOf().doubleType().and()
        .intType().endUnion().doubleDefault(4.0d)
      .name("stringU").type().unionOf().stringType().and()
        .intType().endUnion().stringDefault("def")
      .name("bytesU").type().unionOf().bytesType().and()
        .intType().endUnion().bytesDefault(bytedef)
      .name("nullU").type().unionOf().nullType().and()
        .intType().endUnion().nullDefault()
      .name("fixedU").type().unionOf().fixed("F4").size(1).and()
        .intType().endUnion().fixedDefault(bytedef)
      .name("enumU").type().unionOf().enumeration("E2").symbols("SS").and()
        .intType().endUnion().enumDefault("SS")
      .name("mapU").type().unionOf().map().values().stringType().and()
        .intType().endUnion().mapDefault(mapdef)
      .name("arrayU").type().unionOf().array().items().stringType().and()
        .intType().endUnion().arrayDefault(arrdef)
      .name("recordU").type().unionOf().record("inner2").fields()
        .name("f2").type().intType().noDefault()
        .endRecord().and().intType().endUnion().recordDefault(recdef2)
      .endRecord();

    GenericData.Record newRec =
        new GenericRecordBuilder(r).build();

    Assert.assertEquals(false, newRec.get("boolF"));
    Assert.assertEquals(false, newRec.get("boolU"));
    Assert.assertEquals(1, newRec.get("intF"));
    Assert.assertEquals(1, newRec.get("intU"));
    Assert.assertEquals(2L, newRec.get("longF"));
    Assert.assertEquals(2L, newRec.get("longU"));
    Assert.assertEquals(3f, newRec.get("floatF"));
    Assert.assertEquals(3f, newRec.get("floatU"));
    Assert.assertEquals(4d, newRec.get("doubleF"));
    Assert.assertEquals(4d, newRec.get("doubleU"));
    Assert.assertEquals("def", newRec.get("stringF").toString());
    Assert.assertEquals("def", newRec.get("stringU").toString());
    Assert.assertEquals(bufdef, newRec.get("bytesF1"));
    Assert.assertEquals(bufdef, newRec.get("bytesF2"));
    Assert.assertEquals(bufdef, newRec.get("bytesF3"));
    Assert.assertEquals(bufdef, newRec.get("bytesU"));
    Assert.assertNull(newRec.get("nullF"));
    Assert.assertNull(newRec.get("nullU"));
    Assert.assertArrayEquals(bytedef,
        ((GenericData.Fixed)newRec.get("fixedF1")).bytes());
    Assert.assertArrayEquals(bytedef,
        ((GenericData.Fixed)newRec.get("fixedF2")).bytes());
    Assert.assertArrayEquals(bytedef,
        ((GenericData.Fixed)newRec.get("fixedF3")).bytes());
    Assert.assertArrayEquals(bytedef,
        ((GenericData.Fixed)newRec.get("fixedU")).bytes());
    Assert.assertEquals("S", newRec.get("enumF").toString());
    Assert.assertEquals("SS", newRec.get("enumU").toString());
    @SuppressWarnings("unchecked")
    Map<CharSequence, CharSequence> map =
      (Map<CharSequence, CharSequence>) newRec.get("mapF");
    Assert.assertEquals(mapdef.size(), map.size());
    for(Map.Entry<CharSequence, CharSequence> e : map.entrySet()) {
      Assert.assertEquals(
          mapdef.get(e.getKey().toString()), e.getValue().toString());
    }
    Assert.assertEquals(newRec.get("mapF"), newRec.get("mapU"));
    @SuppressWarnings("unchecked")
    GenericData.Array<CharSequence> arr =
      (GenericData.Array<CharSequence>) newRec.get("arrayF");
    Assert.assertEquals(arrdef.size(), arr.size());
    for(CharSequence c : arr) {
      Assert.assertTrue(arrdef.contains(c.toString()));
    }
    Assert.assertEquals(newRec.get("arrF"), newRec.get("arrU"));
    Assert.assertEquals(recdef, newRec.get("recordF"));
    Assert.assertEquals(recdef2, newRec.get("recordU"));
    Assert.assertEquals("S", newRec.get("byName").toString());
  }

  @Test(expected=SchemaBuilderException.class)
  public void testBadDefault() {
    SchemaBuilder.record("r").fields()
      .name("f").type(Schema.create(Schema.Type.INT)).withDefault(new Object())
      .endRecord();
  }

  @Test
  public void testUnionFieldBuild() {
    SchemaBuilder.record("r").fields()
      .name("allUnion").type().unionOf()
        .booleanType().and()
        .intType().and()
        .longType().and()
        .floatType().and()
        .doubleType().and()
        .stringType().and()
        .bytesType().and()
        .nullType().and()
        .fixed("Fix").size(1).and()
        .enumeration("Enu").symbols("Q").and()
        .array().items().intType().and()
        .map().values().longType().and()
        .record("Rec").fields()
          .name("one").type("Fix").noDefault()
          .endRecord()
        .endUnion().booleanDefault(false)
      .endRecord();
  }

  @Test
  public void testDefaults() throws IOException {
    Schema writeSchema = SchemaBuilder.record("r").fields()
      .name("requiredInt").type().intType().noDefault()
      .name("optionalInt").type().optional().intType()
      .name("nullableIntWithDefault").type().nullable().intType().intDefault(3)
      .endRecord();

    GenericData.Record rec1 = new GenericRecordBuilder(writeSchema)
        .set("requiredInt", 1)
        .build();

    Assert.assertEquals(1, rec1.get("requiredInt"));
    Assert.assertEquals(null, rec1.get("optionalInt"));
    Assert.assertEquals(3, rec1.get("nullableIntWithDefault"));

    GenericData.Record rec2 = new GenericRecordBuilder(writeSchema)
        .set("requiredInt", 1)
        .set("optionalInt", 2)
        .set("nullableIntWithDefault", 13)
        .build();

    Assert.assertEquals(1, rec2.get("requiredInt"));
    Assert.assertEquals(2, rec2.get("optionalInt"));
    Assert.assertEquals(13, rec2.get("nullableIntWithDefault"));

    // write to file
    DataFileWriter<Object> writer =
        new DataFileWriter<Object>(new GenericDatumWriter<Object>());
    writer.create(writeSchema, FILE);
    writer.append(rec1);
    writer.append(rec2);
    writer.close();

    Schema readSchema = SchemaBuilder.record("r").fields()
        .name("requiredInt").type().intType().noDefault()
        .name("optionalInt").type().optional().intType()
        .name("nullableIntWithDefault").type().nullable().intType().intDefault(3)
        .name("newOptionalInt").type().optional().intType()
        .name("newNullableIntWithDefault").type().nullable().intType().intDefault(5)
        .endRecord();

    DataFileReader<GenericData.Record> reader =
        new DataFileReader<GenericData.Record>(FILE,
            new GenericDatumReader<GenericData.Record>(writeSchema, readSchema));

    GenericData.Record rec1read = reader.iterator().next();
    Assert.assertEquals(1, rec1read.get("requiredInt"));
    Assert.assertEquals(null, rec1read.get("optionalInt"));
    Assert.assertEquals(3, rec1read.get("nullableIntWithDefault"));
    Assert.assertEquals(null, rec1read.get("newOptionalInt"));
    Assert.assertEquals(5, rec1read.get("newNullableIntWithDefault"));

    GenericData.Record rec2read = reader.iterator().next();
    Assert.assertEquals(1, rec2read.get("requiredInt"));
    Assert.assertEquals(2, rec2read.get("optionalInt"));
    Assert.assertEquals(13, rec2read.get("nullableIntWithDefault"));
    Assert.assertEquals(null, rec2read.get("newOptionalInt"));
    Assert.assertEquals(5, rec2read.get("newNullableIntWithDefault"));
  }

}
