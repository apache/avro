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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestSchema {
  @Test
  void splitSchemaBuild() {
    Schema s = SchemaBuilder.record("HandshakeRequest").namespace("org.apache.avro.ipc").fields().name("clientProtocol")
        .type().optional().stringType().name("meta").type().optional().map().values().bytesType().endRecord();

    String schemaString = s.toString();
    int mid = schemaString.length() / 2;

    Schema parsedStringSchema = new org.apache.avro.Schema.Parser().parse(s.toString());
    Schema parsedArrayOfStringSchema = new org.apache.avro.Schema.Parser().parse(schemaString.substring(0, mid),
        schemaString.substring(mid));
    assertNotNull(parsedStringSchema);
    assertNotNull(parsedArrayOfStringSchema);
    assertEquals(parsedStringSchema.toString(), parsedArrayOfStringSchema.toString());
  }

  @Test
  void defaultRecordWithDuplicateFieldName() {
    String recordName = "name";
    Schema schema = Schema.createRecord(recordName, "doc", "namespace", false);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("field_name", Schema.create(Type.NULL), null, null));
    fields.add(new Field("field_name", Schema.create(Type.INT), null, null));
    try {
      schema.setFields(fields);
      fail("Should not be able to create a record with duplicate field name.");
    } catch (AvroRuntimeException are) {
      assertTrue(are.getMessage().contains("Duplicate field field_name in record " + recordName));
    }
  }

  @Test
  void createUnionVarargs() {
    List<Schema> types = new ArrayList<>();
    types.add(Schema.create(Type.NULL));
    types.add(Schema.create(Type.LONG));
    Schema expected = Schema.createUnion(types);

    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertEquals(expected, schema);
  }

  @Test
  void recordWithNullDoc() {
    Schema schema = Schema.createRecord("name", null, "namespace", false);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
  }

  @Test
  void recordWithNullNamespace() {
    Schema schema = Schema.createRecord("name", "doc", null, false);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
  }

  @Test
  void emptyRecordSchema() {
    Schema schema = createDefaultRecord();
    String schemaString = schema.toString();
    assertNotNull(schemaString);
  }

  @Test
  void parseEmptySchema() {
    assertThrows(SchemaParseException.class, () -> {
      new Schema.Parser().parse("");
    });
  }

  @Test
  void schemaWithFields() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("field_name1", Schema.create(Type.NULL), null, null));
    fields.add(new Field("field_name2", Schema.create(Type.INT), null, null));
    Schema schema = createDefaultRecord();
    schema.setFields(fields);
    String schemaString = schema.toString();
    assertNotNull(schemaString);
    assertEquals(2, schema.getFields().size());
  }

  @Test
  void schemaWithNullFields() {
    assertThrows(NullPointerException.class, () -> {
      Schema.createRecord("name", "doc", "namespace", false, null);
    });
  }

  @Test
  void isUnionOnUnionWithMultipleElements() {
    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertTrue(schema.isUnion());
  }

  @Test
  void isUnionOnUnionWithOneElement() {
    Schema schema = Schema.createUnion(Schema.create(Type.LONG));
    assertTrue(schema.isUnion());
  }

  @Test
  void isUnionOnRecord() {
    Schema schema = createDefaultRecord();
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnArray() {
    Schema schema = Schema.createArray(Schema.create(Type.LONG));
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnEnum() {
    Schema schema = Schema.createEnum("name", "doc", "namespace", Collections.singletonList("value"));
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnFixed() {
    Schema schema = Schema.createFixed("name", "doc", "space", 10);
    assertFalse(schema.isUnion());
  }

  @Test
  void isUnionOnMap() {
    Schema schema = Schema.createMap(Schema.create(Type.LONG));
    assertFalse(schema.isUnion());
  }

  @Test
  void isNullableOnUnionWithNull() {
    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertTrue(schema.isNullable());
  }

  @Test
  void isNullableOnUnionWithoutNull() {
    Schema schema = Schema.createUnion(Schema.create(Type.LONG));
    assertFalse(schema.isNullable());
  }

  @Test
  void isNullableOnRecord() {
    Schema schema = createDefaultRecord();
    assertFalse(schema.isNullable());
  }

  private Schema createDefaultRecord() {
    return Schema.createRecord("name", "doc", "namespace", false);
  }

  @Test
  void serialization() throws IOException, ClassNotFoundException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        InputStream jsonSchema = getClass().getResourceAsStream("/SchemaBuilder.avsc")) {

      Schema payload = new Schema.Parser().parse(jsonSchema);
      oos.writeObject(payload);

      try (ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
          ObjectInputStream ois = new ObjectInputStream(bis)) {
        Schema sp = (Schema) ois.readObject();
        assertEquals(payload, sp);
      }
    }
  }

  @Test
  void reconstructSchemaStringWithoutInlinedChildReference() {
    String child = "{\"type\":\"record\"," + "\"name\":\"Child\"," + "\"namespace\":\"org.apache.avro.nested\","
        + "\"fields\":" + "[{\"name\":\"childField\",\"type\":\"string\"}]}";
    String parent = "{\"type\":\"record\"," + "\"name\":\"Parent\"," + "\"namespace\":\"org.apache.avro.nested\","
        + "\"fields\":" + "[{\"name\":\"child\",\"type\":\"Child\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema childSchema = parser.parse(child);
    Schema parentSchema = parser.parse(parent);
    String parentWithoutInlinedChildReference = parentSchema.toString(Collections.singleton(childSchema), false);
    // The generated string should be the same as the original parent
    // schema string that did not have the child schema inlined.
    assertEquals(parent, parentWithoutInlinedChildReference);
  }

  @Test
  void intDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1, field.defaultVal());
    assertEquals(1, GenericData.get().getDefaultValue(field));

    field = new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", Integer.MIN_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MIN_VALUE, field.defaultVal());
    assertEquals(Integer.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", Integer.MAX_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MAX_VALUE, field.defaultVal());
    assertEquals(Integer.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  void validLongAsIntDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1, field.defaultVal());
    assertEquals(1, GenericData.get().getDefaultValue(field));

    field = new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", Long.valueOf(Integer.MIN_VALUE));
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MIN_VALUE, field.defaultVal());
    assertEquals(Integer.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", Long.valueOf(Integer.MAX_VALUE));
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MAX_VALUE, field.defaultVal());
    assertEquals(Integer.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  void invalidLongAsIntDefaultValue() {
    assertThrows(AvroTypeException.class, () -> {
      new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", Integer.MAX_VALUE + 1L);
    });
  }

  @Test
  void doubleAsIntDefaultValue() {
    assertThrows(AvroTypeException.class, () -> {
      new Schema.Field("myField", Schema.create(Schema.Type.INT), "doc", 1.0);
    });
  }

  @Test
  void longDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.LONG), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1L, field.defaultVal());
    assertEquals(1L, GenericData.get().getDefaultValue(field));

    field = new Schema.Field("myField", Schema.create(Schema.Type.LONG), "doc", Long.MIN_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Long.MIN_VALUE, field.defaultVal());
    assertEquals(Long.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Schema.Field("myField", Schema.create(Schema.Type.LONG), "doc", Long.MAX_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Long.MAX_VALUE, field.defaultVal());
    assertEquals(Long.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  void intAsLongDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.LONG), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1L, field.defaultVal());
    assertEquals(1L, GenericData.get().getDefaultValue(field));
  }

  @Test
  void doubleAsLongDefaultValue() {
    assertThrows(AvroTypeException.class, () -> {
      new Schema.Field("myField", Schema.create(Schema.Type.LONG), "doc", 1.0);
    });
  }

  @Test
  void doubleDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.DOUBLE), "doc", 1.0);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void intAsDoubleDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.DOUBLE), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void longAsDoubleDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.DOUBLE), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void floatAsDoubleDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.DOUBLE), "doc", 1.0f);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  void floatDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.FLOAT), "doc", 1.0f);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void intAsFloatDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.FLOAT), "doc", 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void longAsFloatDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.FLOAT), "doc", 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void doubleAsFloatDefaultValue() {
    Schema.Field field = new Schema.Field("myField", Schema.create(Schema.Type.FLOAT), "doc", 1.0d);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  void enumSymbolAsNull() {
    assertThrows(SchemaParseException.class, () -> {
      Schema.createEnum("myField", "doc", "namespace", Collections.singletonList(null));
    });
  }

  @Test
  void schemaFieldWithoutSchema() {
    assertThrows(NullPointerException.class, () -> {
      new Schema.Field("f", null);
    });
  }

  @Test
  void parseRecordWithNameAsType() {
    final String schemaString = "{\n  \"type\" : \"record\",\n  \"name\" : \"ns.int\",\n"
        + "  \"fields\" : [ \n    {\"name\" : \"value\", \"type\" : \"int\"}, \n"
        + "    {\"name\" : \"next\", \"type\" : [ \"null\", \"ns.int\" ]}\n  ]\n}";
    final Schema schema = new Schema.Parser().parse(schemaString);
    String toString = schema.toString(true);

    final Schema schema2 = new Schema.Parser().parse(toString);
    assertEquals(schema, schema2);
  }

  @Test
  void qualifiedName() {
    Arrays.stream(Type.values()).forEach((Type t) -> {
      final Schema.Name name = new Schema.Name(t.getName(), "space");
      assertEquals("space." + t.getName(), name.getQualified("space"));
      assertEquals("space." + t.getName(), name.getQualified("otherdefault"));
    });
    final Schema.Name name = new Schema.Name("name", "space");
    assertEquals("name", name.getQualified("space"));
    assertEquals("space.name", name.getQualified("otherdefault"));

    final Schema.Name nameInt = new Schema.Name("Int", "space");
    assertEquals("Int", nameInt.getQualified("space"));
  }

  @Test
  void validValue() {
    // Valid null value
    final Schema nullSchema = Schema.create(Type.NULL);
    assertTrue(nullSchema.isValidDefault(JsonNodeFactory.instance.nullNode()));

    // Valid int value
    final Schema intSchema = Schema.create(Type.INT);
    assertTrue(intSchema.isValidDefault(JsonNodeFactory.instance.numberNode(12)));

    // Valid Text value
    final Schema strSchema = Schema.create(Type.STRING);
    assertTrue(strSchema.isValidDefault(new TextNode("textNode")));

    // Valid Array value
    final Schema arraySchema = Schema.createArray(Schema.create(Type.STRING));
    final ArrayNode arrayValue = JsonNodeFactory.instance.arrayNode();
    assertTrue(arraySchema.isValidDefault(arrayValue)); // empty array

    arrayValue.add("Hello");
    arrayValue.add("World");
    assertTrue(arraySchema.isValidDefault(arrayValue));

    arrayValue.add(5);
    assertFalse(arraySchema.isValidDefault(arrayValue));

    // Valid Union type
    final Schema unionSchema = Schema.createUnion(strSchema, intSchema, nullSchema);
    assertTrue(unionSchema.isValidDefault(JsonNodeFactory.instance.textNode("Hello")));
    assertTrue(unionSchema.isValidDefault(new IntNode(23)));
    assertTrue(unionSchema.isValidDefault(JsonNodeFactory.instance.nullNode()));

    assertFalse(unionSchema.isValidDefault(arrayValue));

    // Array of union
    final Schema arrayUnion = Schema.createArray(unionSchema);
    final ArrayNode arrayUnionValue = JsonNodeFactory.instance.arrayNode();
    arrayUnionValue.add("Hello");
    arrayUnionValue.add(NullNode.getInstance());
    assertTrue(arrayUnion.isValidDefault(arrayUnionValue));

    // Union String, bytes
    final Schema unionStrBytes = Schema.createUnion(strSchema, Schema.create(Type.BYTES));
    assertTrue(unionStrBytes.isValidDefault(JsonNodeFactory.instance.textNode("Hello")));
    assertFalse(unionStrBytes.isValidDefault(JsonNodeFactory.instance.numberNode(123)));
  }

  @Test
  void enumLateDefine() {
    String schemaString = "{\n" + "    \"type\":\"record\",\n" + "    \"name\": \"Main\",\n" + "    \"fields\":[\n"
        + "        {\n" + "            \"name\":\"f1\",\n" + "            \"type\":\"Sub\"\n" + "        },\n"
        + "        {\n" + "            \"name\":\"f2\",\n" + "            \"type\":{\n"
        + "                \"type\":\"enum\",\n" + "                \"name\":\"Sub\",\n"
        + "                \"symbols\":[\"OPEN\",\"CLOSE\"]\n" + "            }\n" + "        }\n" + "    ]\n" + "}";

    final Schema schema = new Schema.Parser().parse(schemaString);
    Schema f1Schema = schema.getField("f1").schema();
    Schema f2Schema = schema.getField("f2").schema();
    assertSame(f1Schema, f2Schema);
    assertEquals(Type.ENUM, f1Schema.getType());
    String stringSchema = schema.toString();
    int definitionIndex = stringSchema.indexOf("\"symbols\":[\"OPEN\",\"CLOSE\"]");
    int usageIndex = stringSchema.indexOf("\"type\":\"Sub\"");
    assertTrue(definitionIndex < usageIndex, "usage is before definition");
  }

  @Test
  public void testRecordInArray() {
    String schemaString = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestRecord\",\n" + "  \"fields\": [\n"
        + "    {\n" + "      \"name\": \"value\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
        + "        \"name\": \"Container\",\n" + "        \"fields\": [\n" + "          {\n"
        + "            \"name\": \"Optional\",\n" + "            \"type\": {\n" + "              \"type\": \"array\",\n"
        + "              \"items\": [\n" + "                {\n" + "                  \"type\": \"record\",\n"
        + "                  \"name\": \"optional_field_0\",\n" + "                  \"namespace\": \"\",\n"
        + "                  \"doc\": \"\",\n" + "                  \"fields\": [\n" + "                    {\n"
        + "                      \"name\": \"optional_field_1\",\n" + "                      \"type\": \"long\",\n"
        + "                      \"doc\": \"\",\n" + "                      \"default\": 0\n"
        + "                    }\n" + "                  ]\n" + "                }\n" + "              ]\n"
        + "            }\n" + "          }\n" + "        ]\n" + "      }\n" + "    }\n" + "  ]\n" + "}";
    final Schema schema = new Schema.Parser().parse(schemaString);
    assertNotNull(schema);
  }

  /*
   * @Test public void testRec() { String schemaString =
   * "[{\"name\":\"employees\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Pair1081149ea1d6eb80\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":{\"type\":\"record\",\"name\":\"EmployeeInfo2\",\"fields\":[{\"name\":\"companyMap\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"PairIntegerString\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]},\"java-class\":\"java.util.HashMap\"}],\"default\":null},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]},\"java-class\":\"java.util.HashMap\"}],\"default\":null}]";
   * final Schema schema = new Schema.Parser().parse(schemaString);
   * Assert.assertNotNull(schema);
   *
   * }
   */

  @Test
  public void testUnionFieldType() {
    String schemaString = "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": [{\"name\":\"value\", \"type\":[\"null\", \"string\",{\"type\": \"record\", \"name\": \"Cons\", \"fields\": [{\"name\":\"car\", \"type\":\"Lisp\"},{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}";
    final Schema schema = new Schema.Parser().parse(schemaString);
    Field value = schema.getField("value");
    Schema fieldSchema = value.schema();
    Schema subSchema = fieldSchema.getTypes().stream().filter((Schema s) -> s.getType() == Type.RECORD).findFirst()
        .get();
    assertTrue(subSchema.hasFields());
  }

  @Test
  public void parseAliases() throws JsonProcessingException {
    String s1 = "{ \"aliases\" : [\"a1\",  \"b1\"]}";
    ObjectMapper mapper = new ObjectMapper();
    JsonNode j1 = mapper.readTree(s1);
    Set<String> aliases = Schema.parseAliases(j1);
    assertEquals(2, aliases.size());
    assertTrue(aliases.contains("a1"));
    assertTrue(aliases.contains("b1"));

    String s2 = "{ \"aliases\" : {\"a1\": \"b1\"}}";
    JsonNode j2 = mapper.readTree(s2);

    SchemaParseException ex = assertThrows(SchemaParseException.class, () -> Schema.parseAliases(j2));
    assertTrue(ex.getMessage().contains("aliases not an array"));

    String s3 = "{ \"aliases\" : [11,  \"b1\"]}";
    JsonNode j3 = mapper.readTree(s3);
    SchemaParseException ex3 = assertThrows(SchemaParseException.class, () -> Schema.parseAliases(j3));
    assertTrue(ex3.getMessage().contains("alias not a string"));
  }

  @Test
  void testContentAfterAvsc() {
    Schema.Parser parser = new Schema.Parser(NameValidator.UTF_VALIDATOR);
    parser.setValidateDefaults(true);
    assertThrows(SchemaParseException.class, () -> parser.parse("{\"type\": \"string\"}; DROP TABLE STUDENTS"));
  }

  @Test
  void testContentAfterAvscInInputStream() throws Exception {
    Schema.Parser parser = new Schema.Parser(NameValidator.UTF_VALIDATOR);
    parser.setValidateDefaults(true);
    String avsc = "{\"type\": \"string\"}; DROP TABLE STUDENTS";
    ByteArrayInputStream is = new ByteArrayInputStream(avsc.getBytes(StandardCharsets.UTF_8));
    Schema schema = parser.parse(is);
    assertNotNull(schema);
  }

  @Test
  void testContentAfterAvscInFile() throws Exception {
    File avscFile = Files.createTempFile("testContentAfterAvscInFile", null).toFile();
    try (FileWriter writer = new FileWriter(avscFile)) {
      writer.write("{\"type\": \"string\"}; DROP TABLE STUDENTS");
      writer.flush();
    }

    Schema.Parser parser = new Schema.Parser(NameValidator.UTF_VALIDATOR);
    parser.setValidateDefaults(true);
    assertThrows(SchemaParseException.class, () -> parser.parse(avscFile));
  }

  @Test
  void testParseMultipleFile() throws IOException {
    URL directory = requireNonNull(Thread.currentThread().getContextClassLoader().getResource("multipleFile"));
    File f1 = new File(directory.getPath(), "ApplicationEvent.avsc");
    File f2 = new File(directory.getPath(), "DocumentInfo.avsc");
    File f3 = new File(directory.getPath(), "MyResponse.avsc");
    Assertions.assertTrue(f1.exists(), "File not exist for test " + f1.getPath());
    Assertions.assertTrue(f2.exists(), "File not exist for test " + f2.getPath());
    Assertions.assertTrue(f3.exists(), "File not exist for test " + f3.getPath());
    SchemaParser parser = new SchemaParser();
    parser.parse(f1);
    parser.parse(f2);
    parser.parse(f3);
    final Map<String, Schema> schemas = parser.getParsedNamedSchemas().stream()
        .collect(Collectors.toMap(Schema::getName, Function.identity()));
    Assertions.assertEquals(3, schemas.size());
    Schema schemaAppEvent = schemas.get("ApplicationEvent");
    Schema schemaDocInfo = schemas.get("DocumentInfo");
    Schema schemaResponse = schemas.get("MyResponse");
    Assertions.assertNotNull(schemaAppEvent);
    Assertions.assertEquals(4, schemaAppEvent.getFields().size());
    Field documents = schemaAppEvent.getField("documents");
    Schema docSchema = documents.schema().getTypes().get(1).getElementType();
    Assertions.assertEquals(docSchema, schemaDocInfo);
    Assertions.assertNotNull(schemaDocInfo);
    Assertions.assertNotNull(schemaResponse);
  }

  @Test
  void add_types() {
    String schemaRecord2 = "{\"type\":\"record\", \"name\":\"record2\", \"fields\": ["
        + "  {\"name\":\"f1\", \"type\":\"record1\" }" + "]}"; // register schema1 in schema.
    Schema schemaRecord1 = Schema.createRecord("record1", "doc", "", false);
    schemaRecord1.setFields(Collections.singletonList(new Field("name", Schema.create(Type.STRING))));
    Schema.Parser parser = new Schema.Parser().addTypes(Collections.singleton(schemaRecord1));

    // parse schema for record2 that contains field for schema1.
    final Schema schema = parser.parse(schemaRecord2);
    final Field f1 = schema.getField("f1");
    assertNotNull(f1);
    assertEquals(schemaRecord1, f1.schema());
  }

  /**
   * Tests the behavior of Schema.Parser if its validation option is set to
   * `null`. This is then set to the default option `NO_VALIDATION`.
   */
  @Test
  void testParserNullValidate() {
    new Schema.Parser((NameValidator) null).parse("{\"type\":\"record\",\"name\":\"\",\"fields\":[]}"); // Empty name
  }

  /**
   * Tests when a user tries to write a record with an invalid enum symbol value
   * that the exception returned is more descriptive than just a NPE or an
   * incorrect mention of an unspecified non-null field.
   */
  @Test
  void enumWriteUnknownField() throws IOException {
    Schema schema = Schema.createRecord("record1", "doc", "", false);
    String goodValue = "HELLO";
    Schema enumSchema = Schema.createEnum("enum1", "doc", "", Arrays.asList(goodValue));
    Field field1 = new Field("field1", enumSchema);
    schema.setFields(Collections.singletonList(field1));

    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    String badValue = "GOODBYE";
    builder.set(field1, new EnumSymbol(enumSchema, badValue));
    Record record = builder.build();
    try {
      datumWriter.write(record, encoder);
      fail("should have thrown");
    } catch (AvroTypeException ate) {
      assertTrue(ate.getMessage().contains(goodValue));
      assertTrue(ate.getMessage().contains(badValue));
    }
  }
}
