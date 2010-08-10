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
package org.apache.avro.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import javax.tools.JavaCompiler.CompilationTask;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.TestProtocolParsing;
import org.apache.avro.TestSchema;
import org.apache.avro.specific.SpecificCompiler.OutputFile;
import org.junit.Test;


public class TestSpecificCompiler {
  @Test
  public void testEsc() {
    assertEquals("\\\"", SpecificCompiler.esc("\""));
  }

  @Test
  public void testMakePath() {
    assertEquals("foo/bar/Baz.java".replace("/", File.separator), SpecificCompiler.makePath("Baz", "foo.bar"));
    assertEquals("baz.java", SpecificCompiler.makePath("baz", ""));
  }

  @Test
  public void testPrimitiveSchemaGeneratesNothing() {
    assertEquals(0, new SpecificCompiler(Schema.parse("\"double\"")).compile().size());
  }

  @Test
  public void testSimpleEnumSchema() throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(Schema.parse(TestSchema.BASIC_ENUM_SCHEMA)).compile();
    assertEquals(1, outputs.size());
    OutputFile o = outputs.iterator().next();
    assertEquals(o.path, "Test.java");
    assertTrue(o.contents.contains("public enum Test"));
    assertCompilesWithJavaCompiler(outputs);
  }

  @Test
  public void testMangleIfReserved() {
    assertEquals("foo", SpecificCompiler.mangle("foo"));
    assertEquals("goto$", SpecificCompiler.mangle("goto"));
  }

  @Test
  public void testManglingForProtocols() throws IOException {
    String protocolDef = "" +
      "{ \"protocol\": \"default\",\n" +
      "  \"types\":\n" +
      "    [\n" +
      "      {\n" +
      "       \"name\": \"finally\",\n" +
      "       \"type\": \"error\",\n" +
      "       \"fields\": [{\"name\": \"catch\", \"type\": \"boolean\"}]\n" +
      "      }\n" +
      "    ],\n" +
      "  \"messages\": { \"goto\":\n" +
      "    { \"request\": [{\"name\": \"break\", \"type\": \"string\"}],\n" +
      "      \"response\": \"string\",\n" +
      "      \"errors\": [\"finally\"]\n" +
      "    }" +
      "   }\n" +
      "}\n";
    Collection<OutputFile> c =
      new SpecificCompiler(Protocol.parse(protocolDef)).compile();
    Iterator<OutputFile> i = c.iterator();
    String errType = i.next().contents;
    String protocol = i.next().contents;

    assertTrue(errType.contains("public class finally$ extends org.apache.avro.specific.SpecificExceptionBase"));
    assertTrue(errType.contains("public boolean catch$;"));

    assertTrue(protocol.contains("java.lang.CharSequence goto$(java.lang.CharSequence break$)"));
    assertTrue(protocol.contains("public interface default$"));
    assertTrue(protocol.contains("throws org.apache.avro.ipc.AvroRemoteException, finally$"));
    
    assertCompilesWithJavaCompiler(c);

  }

  @Test
  public void testManglingForRecords() throws IOException {
    String schema = "" +
      "{ \"name\": \"volatile\", \"type\": \"record\", " +
      "  \"fields\": [ {\"name\": \"package\", \"type\": \"string\" }," +
      "                {\"name\": \"short\", \"type\": \"volatile\" } ] }";
    Collection<OutputFile> c =
      new SpecificCompiler(Schema.parse(schema)).compile();
    assertEquals(1, c.size());
    String contents = c.iterator().next().contents;

    assertTrue(contents.contains("public java.lang.CharSequence package$;"));
    assertTrue(contents.contains("class volatile$ extends"));
    assertTrue(contents.contains("volatile$ short$;"));
    
    assertCompilesWithJavaCompiler(c);
  }

  @Test
  public void testManglingForEnums() throws IOException {
    String enumSchema = "" +
      "{ \"name\": \"instanceof\", \"type\": \"enum\"," +
      "  \"symbols\": [\"new\", \"super\", \"switch\"] }";
    Collection<OutputFile> c =
      new SpecificCompiler(Schema.parse(enumSchema)).compile();
    assertEquals(1, c.size());
    String contents = c.iterator().next().contents;

    assertTrue(contents.contains("new$"));
    
    assertCompilesWithJavaCompiler(c);
  }

  @Test
  public void testSchemaWithDocs() {
    Collection<OutputFile> outputs = new SpecificCompiler(
        Schema.parse(TestSchema.SCHEMA_WITH_DOC_TAGS)).compile();
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
  public void testProtocolWithDocs() throws IOException {
    Protocol protocol = TestProtocolParsing.getSimpleProtocol();
    Collection<OutputFile> out = new SpecificCompiler(protocol).compile();
    assertEquals(5, out.size());
    int count = 0;
    for (OutputFile o : out) {
      if (o.path.endsWith("Simple.java")) {
        count++;
        assertTrue(o.contents.contains("/** Protocol used for testing. */"));
        assertTrue(o.contents.contains("/** Send a greeting */"));
      }
    }
    assertEquals("Missed generated protocol!", 1, count);
  }
  
  @Test
  public void testNeedCompile() throws IOException, InterruptedException {
    String schema = "" +
      "{ \"name\": \"Foo\", \"type\": \"record\", " +
      "  \"fields\": [ {\"name\": \"package\", \"type\": \"string\" }," +
      "                {\"name\": \"short\", \"type\": \"Foo\" } ] }";
    File inputFile = File.createTempFile("input", "avsc");
    FileWriter fw = new FileWriter(inputFile);
    fw.write(schema);
    fw.close();
    
    File outputDir = new File(System.getProperty("test.dir") + 
      System.getProperty("file.separator") + "test_need_compile");
    File outputFile = new File(outputDir, "Foo.java");
    outputFile.delete();
    assertTrue(!outputFile.exists());
    outputDir.delete();
    assertTrue(!outputDir.exists());
    SpecificCompiler.compileSchema(inputFile, outputDir);
    assertTrue(outputDir.exists());
    assertTrue(outputFile.exists());

    long lastModified = outputFile.lastModified();
    Thread.sleep(1000);  //granularity of JVM doesn't seem to go below 1 sec
    SpecificCompiler.compileSchema(inputFile, outputDir);
    assertEquals(lastModified, outputFile.lastModified());
    
    fw = new FileWriter(inputFile);
    fw.write(schema);
    fw.close();
    SpecificCompiler.compileSchema(inputFile, outputDir);
    assertTrue(lastModified != outputFile.lastModified());
  }

  /**
   * Checks that a schema passes through the SpecificCompiler, and,
   * optionally, uses the system's Java compiler to check
   * that the generated code is valid.
   */
  public static void
      assertCompiles(Schema schema, boolean useJavaCompiler) 
  throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(schema).compile();
    assertTrue(null != outputs);
    if (useJavaCompiler) {
      assertCompilesWithJavaCompiler(outputs);
    }
  }
  
  /**
   * Checks that a protocol passes through the SpecificCompiler,
   * and, optionally, uses the system's Java compiler to check
   * that the generated code is valid.
   */
  public static void assertCompiles(Protocol protocol, boolean useJavaCompiler)
  throws IOException {
    Collection<OutputFile> outputs = new SpecificCompiler(protocol).compile();
    assertTrue(null != outputs);
    if (useJavaCompiler) {
      assertCompilesWithJavaCompiler(outputs);
    }
  }
  
  /** Uses the system's java compiler to actually compile the generated code. */
  static void assertCompilesWithJavaCompiler(Collection<OutputFile> outputs) 
  throws IOException {
    if (outputs.isEmpty()) {
      return;               // Nothing to compile!
    }
    File dstDir = AvroTestUtil.tempFile("realCompiler");
    List<File> javaFiles = new ArrayList<File>();
    for (OutputFile o : outputs) {
      javaFiles.add(o.writeToDestination(null, dstDir));
    }

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = 
      compiler.getStandardFileManager(null, null, null);
    
    CompilationTask cTask = compiler.getTask(null, fileManager, null, null, 
        null,
        fileManager.getJavaFileObjects(
            javaFiles.toArray(new File[javaFiles.size()])));
    assertTrue(cTask.call());
  }
}
