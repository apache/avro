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
package org.apache.avro.compiler.specific;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import javax.tools.JavaCompiler.CompilationTask;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.TestProtocolParsing;
import org.apache.avro.TestSchema;
import org.apache.avro.TestAnnotation;
import org.apache.avro.generic.GenericData.StringType;

import org.apache.avro.test.Simple;
import org.apache.avro.test.TestRecord;
import org.apache.avro.test.MD5;
import org.apache.avro.test.Kind;

import org.apache.avro.compiler.specific.SpecificCompiler.OutputFile;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TestName;

public class TestSpecificCompiler {

  @TempDir
  public File INPUT_DIR;

  @TempDir
  public File OUTPUT_DIR;

  static final String PROTOCOL = "" + "{ \"protocol\": \"default\",\n" + "  \"types\":\n" + "    [\n" + "      {\n"
      + "       \"name\": \"finally\",\n" + "       \"type\": \"error\",\n"
      + "       \"fields\": [{\"name\": \"catch\", \"type\": \"boolean\"}]\n" + "      }\n" + "    ],\n"
      + "  \"messages\": { \"goto\":\n" + "    { \"request\": [{\"name\": \"break\", \"type\": \"string\"}],\n"
      + "      \"response\": \"string\",\n" + "      \"errors\": [\"finally\"]\n" + "    }" + "   }\n" + "}\n";

  @Test
  void esc() {
    assertEquals("\\\"", SpecificCompiler.javaEscape("\""));
  }

  @Test
  void makePath() {
    SpecificCompiler compiler = new SpecificCompiler();
    assertEquals("foo/bar/Baz.java".replace("/", File.separator), compiler.makePath("Baz", "foo.bar"));
    assertEquals("baz.java", compiler.makePath("baz", ""));
  }

  @Test
  void primitiveSchemaGeneratesNothing() {
    assertEquals(0, new SpecificCompiler(new Schema.Parser().parse("\"double\"")).compile().size());
  }

  @Test
  void simpleEnumSchema(TestInfo testInfo) throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(new Schema.Parser().parse(TestSchema.BASIC_ENUM_SCHEMA))
        .compile();
    assertEquals(1, outputs.size());
    OutputFile o = outputs.iterator().next();
    assertEquals(o.path, "Test.java");
    assertTrue(o.contents.contains("public enum Test"));
    assertCompilesWithJavaCompiler(new File(INPUT_DIR, testInfo.getTestMethod().get().getName()), outputs);
  }

  @Test
  void mangleIfReserved() {
    assertEquals("foo", SpecificCompiler.mangle("foo"));
    assertEquals("goto$", SpecificCompiler.mangle("goto"));
  }

  @Test
  void manglingForProtocols(TestInfo testInfo) throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(Protocol.parse(PROTOCOL)).compile();
    Iterator<OutputFile> i = outputs.iterator();
    String errType = i.next().contents;
    String protocol = i.next().contents;

    assertTrue(errType.contains("public class finally$ extends org.apache.avro.specific.SpecificExceptionBase"));
    assertTrue(errType.contains("private boolean catch$;"));

    assertTrue(protocol.contains("java.lang.CharSequence goto$(java.lang.CharSequence break$)"));
    assertTrue(protocol.contains("public interface default$"));
    assertTrue(protocol.contains(" finally$"));

    assertCompilesWithJavaCompiler(new File(INPUT_DIR, testInfo.getTestMethod().get().getName()), outputs);

  }

  private static String SCHEMA = "{ \"name\": \"volatile\", \"type\": \"record\", "
      + "  \"fields\": [ {\"name\": \"package\", \"type\": \"string\" },"
      + "                {\"name\": \"data\", \"type\": \"int\" },"
      + "                {\"name\": \"value\", \"type\": \"int\" },"
      + "                {\"name\": \"defaultValue\", \"type\": \"int\" },"
      + "                {\"name\": \"other\", \"type\": \"int\" },"
      + "                {\"name\": \"short\", \"type\": \"volatile\" } ] }";

  @Test
  void manglingForRecords(TestInfo testInfo) throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(new Schema.Parser().parse(SCHEMA)).compile();
    assertEquals(1, outputs.size());
    String contents = outputs.iterator().next().contents;

    assertTrue(contents.contains("private java.lang.CharSequence package$;"));
    assertTrue(contents.contains("class volatile$ extends"));
    assertTrue(contents.contains("volatile$ short$;"));

    assertCompilesWithJavaCompiler(new File(INPUT_DIR, testInfo.getTestMethod().get().getName()), outputs);
  }

  @Test
  void manglingForEnums(TestInfo testInfo) throws IOException {
    String enumSchema = "" + "{ \"name\": \"instanceof\", \"type\": \"enum\","
        + "  \"symbols\": [\"new\", \"super\", \"switch\"] }";
    Collection<OutputFile> outputs = new SpecificCompiler(new Schema.Parser().parse(enumSchema)).compile();
    assertEquals(1, outputs.size());
    String contents = outputs.iterator().next().contents;

    assertTrue(contents.contains("new$"));

    assertCompilesWithJavaCompiler(new File(INPUT_DIR, testInfo.getTestMethod().get().getName()), outputs);
  }

  @Test
  void schemaSplit(TestInfo testInfo) throws IOException {
    SpecificCompiler compiler = new SpecificCompiler(new Schema.Parser().parse(SCHEMA));
    compiler.maxStringChars = 10;
    Collection<OutputFile> files = compiler.compile();
    assertCompilesWithJavaCompiler(new File(INPUT_DIR, testInfo.getTestMethod().get().getName()), files);
  }

  @Test
  void protocolSplit(TestInfo testInfo) throws IOException {
    SpecificCompiler compiler = new SpecificCompiler(Protocol.parse(PROTOCOL));
    compiler.maxStringChars = 10;
    Collection<OutputFile> files = compiler.compile();
    assertCompilesWithJavaCompiler(new File(INPUT_DIR, testInfo.getTestMethod().get().getName()), files);
  }

  @Test
  void schemaWithDocs() {
    Collection<OutputFile> outputs = new SpecificCompiler(new Schema.Parser().parse(TestSchema.SCHEMA_WITH_DOC_TAGS))
        .compile();
    assertEquals(3, outputs.size());
    int count = 0;
    for (OutputFile o : outputs) {
      if (o.path.endsWith("outer_record.java")) {
        count++;
        assertTrue(o.contents.contains("/** This is not a world record. */"));
        assertTrue(o.contents.contains("/** Inner Fixed */"));
        assertTrue(o.contents.contains("/** Inner Enum */"));
        assertTrue(o.contents.contains("/** Inner String */"));
      }
      if (o.path.endsWith("very_inner_fixed.java")) {
        count++;
        assertTrue(o.contents.contains("/** Very Inner Fixed */"));
        assertTrue(o.contents.contains("@org.apache.avro.specific.FixedSize(1)"));
      }
      if (o.path.endsWith("very_inner_enum.java")) {
        count++;
        assertTrue(o.contents.contains("/** Very Inner Enum */"));
      }
    }

    assertEquals(3, count);
  }

  @Test
  void protocolWithDocs() throws IOException {
    Protocol protocol = TestProtocolParsing.getSimpleProtocol();
    Collection<OutputFile> out = new SpecificCompiler(protocol).compile();
    assertEquals(6, out.size());
    int count = 0;
    for (OutputFile o : out) {
      if (o.path.endsWith("Simple.java")) {
        count++;
        assertTrue(o.contents.contains("/** Protocol used for testing. */"));
        assertTrue(o.contents.contains("* Send a greeting"));
      }
    }
    assertEquals(1, count, "Missed generated protocol!");
  }

  @Test
  void needCompile() throws IOException, InterruptedException {
    String schema = "" + "{ \"name\": \"Foo\", \"type\": \"record\", "
        + "  \"fields\": [ {\"name\": \"package\", \"type\": \"string\" },"
        + "                {\"name\": \"short\", \"type\": \"Foo\" } ] }";
    File inputFile = new File(INPUT_DIR.getPath(), "input.avsc");
    try (FileWriter fw = new FileWriter(inputFile)) {
      fw.write(schema);
    }

    File outputDir = OUTPUT_DIR;

    File outputFile = new File(outputDir, "Foo.java");
    outputFile.delete();
    assertFalse(outputFile.exists());
    outputDir.delete();
    assertFalse(outputDir.exists());
    SpecificCompiler.compileSchema(inputFile, outputDir);
    assertTrue(outputDir.exists());
    assertTrue(outputFile.exists());

    long lastModified = outputFile.lastModified();
    Thread.sleep(1000); // granularity of JVM doesn't seem to go below 1 sec
    SpecificCompiler.compileSchema(inputFile, outputDir);
    assertEquals(lastModified, outputFile.lastModified());

    try (FileWriter fw = new FileWriter(inputFile)) {
      fw.write(schema);
    }
    SpecificCompiler.compileSchema(inputFile, outputDir);
    assertTrue(lastModified != outputFile.lastModified());
  }

  /**
   * Creates a record with the given name, error status, and fields.
   *
   * @param name    the name of the schema.
   * @param isError true if the schema represents an error; false otherwise.
   * @param fields  the field(s) to add to the schema.
   * @return the schema.
   */
  private Schema createRecord(String name, boolean isError, Field... fields) {
    Schema record = Schema.createRecord(name, null, null, isError);
    record.setFields(Arrays.asList(fields));
    return record;
  }

  @Test
  void generateGetMethod() {
    Field height = new Field("height", Schema.create(Type.INT), null, null);
    Field Height = new Field("Height", Schema.create(Type.INT), null, null);
    Field height_and_width = new Field("height_and_width", Schema.create(Type.STRING), null, null);
    Field message = new Field("message", Schema.create(Type.STRING), null, null);
    Field Message = new Field("Message", Schema.create(Type.STRING), null, null);
    Field cause = new Field("cause", Schema.create(Type.STRING), null, null);
    Field clasz = new Field("class", Schema.create(Type.STRING), null, null);
    Field schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Field Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);

    assertEquals("getHeight", SpecificCompiler.generateGetMethod(createRecord("test", false, height), height));

    assertEquals("getHeightAndWidth",
        SpecificCompiler.generateGetMethod(createRecord("test", false, height_and_width), height_and_width));

    assertEquals("getMessage", SpecificCompiler.generateGetMethod(createRecord("test", false, message), message));
    message = new Field("message", Schema.create(Type.STRING), null, null);
    assertEquals("getMessage$", SpecificCompiler.generateGetMethod(createRecord("test", true, message), message));

    assertEquals("getCause", SpecificCompiler.generateGetMethod(createRecord("test", false, cause), cause));
    cause = new Field("cause", Schema.create(Type.STRING), null, null);
    assertEquals("getCause$", SpecificCompiler.generateGetMethod(createRecord("test", true, cause), cause));

    assertEquals("getClass$", SpecificCompiler.generateGetMethod(createRecord("test", false, clasz), clasz));
    clasz = new Field("class", Schema.create(Type.STRING), null, null);
    assertEquals("getClass$", SpecificCompiler.generateGetMethod(createRecord("test", true, clasz), clasz));

    assertEquals("getSchema$", SpecificCompiler.generateGetMethod(createRecord("test", false, schema), schema));
    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    assertEquals("getSchema$", SpecificCompiler.generateGetMethod(createRecord("test", true, schema), schema));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("getHeight", SpecificCompiler.generateGetMethod(createRecord("test", false, Height), Height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("getHeight", SpecificCompiler.generateGetMethod(createRecord("test", false, height, Height), height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("getHeight$0",
        SpecificCompiler.generateGetMethod(createRecord("test", false, height, Height), Height));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("getMessage$", SpecificCompiler.generateGetMethod(createRecord("test", true, Message), Message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("getMessage$",
        SpecificCompiler.generateGetMethod(createRecord("test", true, message, Message), message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("getMessage$0",
        SpecificCompiler.generateGetMethod(createRecord("test", true, message, Message), Message));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("getSchema$", SpecificCompiler.generateGetMethod(createRecord("test", false, Schema$), Schema$));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("getSchema$",
        SpecificCompiler.generateGetMethod(createRecord("test", false, schema, Schema$), schema));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("getSchema$0",
        SpecificCompiler.generateGetMethod(createRecord("test", false, schema, Schema$), Schema$));
  }

  @Test
  void generateSetMethod() {
    Field height = new Field("height", Schema.create(Type.INT), null, null);
    Field Height = new Field("Height", Schema.create(Type.INT), null, null);
    Field height_and_width = new Field("height_and_width", Schema.create(Type.STRING), null, null);
    Field message = new Field("message", Schema.create(Type.STRING), null, null);
    Field Message = new Field("Message", Schema.create(Type.STRING), null, null);
    Field cause = new Field("cause", Schema.create(Type.STRING), null, null);
    Field clasz = new Field("class", Schema.create(Type.STRING), null, null);
    Field schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Field Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);

    assertEquals("setHeight", SpecificCompiler.generateSetMethod(createRecord("test", false, height), height));

    assertEquals("setHeightAndWidth",
        SpecificCompiler.generateSetMethod(createRecord("test", false, height_and_width), height_and_width));

    assertEquals("setMessage", SpecificCompiler.generateSetMethod(createRecord("test", false, message), message));
    message = new Field("message", Schema.create(Type.STRING), null, null);
    assertEquals("setMessage$", SpecificCompiler.generateSetMethod(createRecord("test", true, message), message));

    assertEquals("setCause", SpecificCompiler.generateSetMethod(createRecord("test", false, cause), cause));
    cause = new Field("cause", Schema.create(Type.STRING), null, null);
    assertEquals("setCause$", SpecificCompiler.generateSetMethod(createRecord("test", true, cause), cause));

    assertEquals("setClass$", SpecificCompiler.generateSetMethod(createRecord("test", false, clasz), clasz));
    clasz = new Field("class", Schema.create(Type.STRING), null, null);
    assertEquals("setClass$", SpecificCompiler.generateSetMethod(createRecord("test", true, clasz), clasz));

    assertEquals("setSchema$", SpecificCompiler.generateSetMethod(createRecord("test", false, schema), schema));
    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    assertEquals("setSchema$", SpecificCompiler.generateSetMethod(createRecord("test", true, schema), schema));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("setHeight", SpecificCompiler.generateSetMethod(createRecord("test", false, Height), Height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("setHeight", SpecificCompiler.generateSetMethod(createRecord("test", false, height, Height), height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("setHeight$0",
        SpecificCompiler.generateSetMethod(createRecord("test", false, height, Height), Height));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("setMessage$", SpecificCompiler.generateSetMethod(createRecord("test", true, Message), Message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("setMessage$",
        SpecificCompiler.generateSetMethod(createRecord("test", true, message, Message), message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("setMessage$0",
        SpecificCompiler.generateSetMethod(createRecord("test", true, message, Message), Message));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("setSchema$", SpecificCompiler.generateSetMethod(createRecord("test", false, Schema$), Schema$));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("setSchema$",
        SpecificCompiler.generateSetMethod(createRecord("test", false, schema, Schema$), schema));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("setSchema$0",
        SpecificCompiler.generateSetMethod(createRecord("test", false, schema, Schema$), Schema$));
  }

  @Test
  void generateHasMethod() {
    Field height = new Field("height", Schema.create(Type.INT), null, null);
    Field Height = new Field("Height", Schema.create(Type.INT), null, null);
    Field height_and_width = new Field("height_and_width", Schema.create(Type.STRING), null, null);
    Field message = new Field("message", Schema.create(Type.STRING), null, null);
    Field Message = new Field("Message", Schema.create(Type.STRING), null, null);
    Field cause = new Field("cause", Schema.create(Type.STRING), null, null);
    Field clasz = new Field("class", Schema.create(Type.STRING), null, null);
    Field schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Field Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);

    assertEquals("hasHeight", SpecificCompiler.generateHasMethod(createRecord("test", false, height), height));

    assertEquals("hasHeightAndWidth",
        SpecificCompiler.generateHasMethod(createRecord("test", false, height_and_width), height_and_width));

    assertEquals("hasMessage", SpecificCompiler.generateHasMethod(createRecord("test", false, message), message));
    message = new Field("message", Schema.create(Type.STRING), null, null);
    assertEquals("hasMessage$", SpecificCompiler.generateHasMethod(createRecord("test", true, message), message));

    assertEquals("hasCause", SpecificCompiler.generateHasMethod(createRecord("test", false, cause), cause));
    cause = new Field("cause", Schema.create(Type.STRING), null, null);
    assertEquals("hasCause$", SpecificCompiler.generateHasMethod(createRecord("test", true, cause), cause));

    assertEquals("hasClass$", SpecificCompiler.generateHasMethod(createRecord("test", false, clasz), clasz));
    clasz = new Field("class", Schema.create(Type.STRING), null, null);
    assertEquals("hasClass$", SpecificCompiler.generateHasMethod(createRecord("test", true, clasz), clasz));

    assertEquals("hasSchema$", SpecificCompiler.generateHasMethod(createRecord("test", false, schema), schema));
    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    assertEquals("hasSchema$", SpecificCompiler.generateHasMethod(createRecord("test", true, schema), schema));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("hasHeight", SpecificCompiler.generateHasMethod(createRecord("test", false, Height), Height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("hasHeight", SpecificCompiler.generateHasMethod(createRecord("test", false, height, Height), height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("hasHeight$0",
        SpecificCompiler.generateHasMethod(createRecord("test", false, height, Height), Height));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("hasMessage$", SpecificCompiler.generateHasMethod(createRecord("test", true, Message), Message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("hasMessage$",
        SpecificCompiler.generateHasMethod(createRecord("test", true, message, Message), message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("hasMessage$0",
        SpecificCompiler.generateHasMethod(createRecord("test", true, message, Message), Message));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("hasSchema$", SpecificCompiler.generateHasMethod(createRecord("test", false, Schema$), Schema$));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("hasSchema$",
        SpecificCompiler.generateHasMethod(createRecord("test", false, schema, Schema$), schema));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("hasSchema$0",
        SpecificCompiler.generateHasMethod(createRecord("test", false, schema, Schema$), Schema$));
  }

  @Test
  void generateClearMethod() {
    Field height = new Field("height", Schema.create(Type.INT), null, null);
    Field Height = new Field("Height", Schema.create(Type.INT), null, null);
    Field height_and_width = new Field("height_and_width", Schema.create(Type.STRING), null, null);
    Field message = new Field("message", Schema.create(Type.STRING), null, null);
    Field Message = new Field("Message", Schema.create(Type.STRING), null, null);
    Field cause = new Field("cause", Schema.create(Type.STRING), null, null);
    Field clasz = new Field("class", Schema.create(Type.STRING), null, null);
    Field schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Field Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);

    assertEquals("clearHeight", SpecificCompiler.generateClearMethod(createRecord("test", false, height), height));

    assertEquals("clearHeightAndWidth",
        SpecificCompiler.generateClearMethod(createRecord("test", false, height_and_width), height_and_width));

    assertEquals("clearMessage", SpecificCompiler.generateClearMethod(createRecord("test", false, message), message));
    message = new Field("message", Schema.create(Type.STRING), null, null);
    assertEquals("clearMessage$", SpecificCompiler.generateClearMethod(createRecord("test", true, message), message));

    assertEquals("clearCause", SpecificCompiler.generateClearMethod(createRecord("test", false, cause), cause));
    cause = new Field("cause", Schema.create(Type.STRING), null, null);
    assertEquals("clearCause$", SpecificCompiler.generateClearMethod(createRecord("test", true, cause), cause));

    assertEquals("clearClass$", SpecificCompiler.generateClearMethod(createRecord("test", false, clasz), clasz));
    clasz = new Field("class", Schema.create(Type.STRING), null, null);
    assertEquals("clearClass$", SpecificCompiler.generateClearMethod(createRecord("test", true, clasz), clasz));

    assertEquals("clearSchema$", SpecificCompiler.generateClearMethod(createRecord("test", false, schema), schema));
    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    assertEquals("clearSchema$", SpecificCompiler.generateClearMethod(createRecord("test", true, schema), schema));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("clearHeight", SpecificCompiler.generateClearMethod(createRecord("test", false, Height), Height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("clearHeight",
        SpecificCompiler.generateClearMethod(createRecord("test", false, height, Height), height));

    height = new Field("height", Schema.create(Type.INT), null, null);
    Height = new Field("Height", Schema.create(Type.INT), null, null);
    assertEquals("clearHeight$0",
        SpecificCompiler.generateClearMethod(createRecord("test", false, height, Height), Height));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("clearMessage$", SpecificCompiler.generateClearMethod(createRecord("test", true, Message), Message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("clearMessage$",
        SpecificCompiler.generateClearMethod(createRecord("test", true, message, Message), message));

    message = new Field("message", Schema.create(Type.STRING), null, null);
    Message = new Field("Message", Schema.create(Type.STRING), null, null);
    assertEquals("clearMessage$0",
        SpecificCompiler.generateClearMethod(createRecord("test", true, message, Message), Message));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("clearSchema$", SpecificCompiler.generateClearMethod(createRecord("test", false, Schema$), Schema$));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("clearSchema$",
        SpecificCompiler.generateClearMethod(createRecord("test", false, schema, Schema$), schema));

    schema = new Field("schema", Schema.create(Type.STRING), null, null);
    Schema$ = new Field("Schema", Schema.create(Type.STRING), null, null);
    assertEquals("clearSchema$0",
        SpecificCompiler.generateClearMethod(createRecord("test", false, schema, Schema$), Schema$));
  }

  @Test
  void annotations() throws Exception {
    // an interface generated for protocol
    assertNotNull(Simple.class.getAnnotation(TestAnnotation.class));
    // a class generated for a record
    assertNotNull(TestRecord.class.getAnnotation(TestAnnotation.class));
    // a class generated for a fixed
    assertNotNull(MD5.class.getAnnotation(TestAnnotation.class));
    // a class generated for an enum
    assertNotNull(Kind.class.getAnnotation(TestAnnotation.class));

    // a field
    assertNotNull(TestRecord.class.getDeclaredField("name").getAnnotation(TestAnnotation.class));
    // a method
    assertNotNull(Simple.class.getMethod("ack").getAnnotation(TestAnnotation.class));
  }

  @Test
  void aliases() throws IOException {
    Schema s = new Schema.Parser().parse("{\"name\":\"X\",\"type\":\"record\",\"aliases\":[\"Y\"],\"fields\":["
        + "{\"name\":\"f\",\"type\":\"int\",\"aliases\":[\"g\"]}]}");
    SpecificCompiler compiler = new SpecificCompiler(s);
    compiler.setStringType(StringType.valueOf("String"));
    Collection<OutputFile> outputs = compiler.compile();
    assertEquals(1, outputs.size());
    OutputFile o = outputs.iterator().next();
    assertEquals(o.path, "X.java");
    assertTrue(o.contents.contains("[\\\"Y\\\"]"));
    assertTrue(o.contents.contains("[\\\"g\\\"]"));
  }

  /**
   * Checks that a schema passes through the SpecificCompiler, and, optionally,
   * uses the system's Java compiler to check that the generated code is valid.
   */
  public static void assertCompiles(File dstDir, Schema schema, boolean useJavaCompiler) throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(schema).compile();
    assertNotNull(outputs);
    if (useJavaCompiler) {
      assertCompilesWithJavaCompiler(dstDir, outputs);
    }
  }

  /**
   * Checks that a protocol passes through the SpecificCompiler, and, optionally,
   * uses the system's Java compiler to check that the generated code is valid.
   */
  public static void assertCompiles(File dstDir, Protocol protocol, boolean useJavaCompiler) throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(protocol).compile();
    assertNotNull(outputs);
    if (useJavaCompiler) {
      assertCompilesWithJavaCompiler(dstDir, outputs);
    }
  }

  /** Uses the system's java compiler to actually compile the generated code. */
  static void assertCompilesWithJavaCompiler(File dstDir, Collection<OutputFile> outputs) throws IOException {
    if (outputs.isEmpty()) {
      return; // Nothing to compile!
    }

    List<File> javaFiles = new ArrayList<>();
    for (OutputFile o : outputs) {
      javaFiles.add(o.writeToDestination(null, dstDir));
    }

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);

    CompilationTask cTask = compiler.getTask(null, fileManager, null, null, null,
        fileManager.getJavaFileObjects(javaFiles.toArray(new File[0])));
    assertTrue(cTask.call());
  }

  private static String SCHEMA1 = "{ \"name\": \"volatile\", \"type\": \"record\", "
      + "  \"fields\": [{\"name\": \"ownerAddress\", \"type\": [\"null\",{ \"type\": \"string\",\"java-class\": \"java.net.URI\"}], \"default\": null},"
      + "                {\"name\": \"ownerURL\", \"type\": [\"null\",{ \"type\": \"string\",\"java-class\": \"java.net.URL\"}], \"default\": null}]}";

  @Test
  void generateExceptionCodeBlock() throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(new Schema.Parser().parse(SCHEMA1)).compile();
    assertEquals(1, outputs.size());
    String contents = outputs.iterator().next().contents;

    assertTrue(contents.contains("private java.net.URI"));
    assertTrue(contents.contains("catch (java.net.URISyntaxException e)"));
    assertTrue(contents.contains("private java.net.URL"));
    assertTrue(contents.contains("catch (java.net.MalformedURLException e)"));
  }
}
