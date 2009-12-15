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
import java.util.Collection;
import java.util.Iterator;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.TestSchema;
import org.apache.avro.specific.SpecificCompiler.OutputFile;
import org.junit.Test;


public class TestSpecificCompiler {
  @Test
  public void testEsc() {
    assertEquals("\\\"", SpecificCompiler.esc("\""));
  }

  @Test
  public void testCap() {
    assertEquals("Foobar", SpecificCompiler.cap("foobar"));
    assertEquals("F", SpecificCompiler.cap("f"));
    assertEquals("F", SpecificCompiler.cap("F"));
  }

  @Test
  public void testMakePath() {
    assertEquals("foo/bar/Baz.java".replace("/", File.separator), SpecificCompiler.makePath("baz", "foo.bar"));
    assertEquals("Baz.java", SpecificCompiler.makePath("baz", ""));
  }

  @Test
  public void testPrimitiveSchemaGeneratesNothing() {
    assertEquals(0, new SpecificCompiler(Schema.parse("\"double\"")).compile().size());
  }

  @Test
  public void testSimpleEnumSchema() {
    Collection<OutputFile> outputs = new SpecificCompiler(Schema.parse(TestSchema.BASIC_ENUM_SCHEMA)).compile();
    assertEquals(1, outputs.size());
    OutputFile o = outputs.iterator().next();
    assertEquals(o.path, "Test.java");
    assertTrue(o.contents.contains("public enum Test"));
  }

  @Test
  public void testMangleIfReserved() {
    assertEquals("foo", SpecificCompiler.mangle("foo"));
    assertEquals("goto$", SpecificCompiler.mangle("goto"));
  }

  @Test
  public void testManglingForProtocols() {
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
    Iterator<OutputFile> i =
      new SpecificCompiler(Protocol.parse(protocolDef)).compile().iterator();
    String errType = i.next().contents;
    String protocol = i.next().contents;

    assertTrue(errType.contains("public class finally$ extends org.apache.avro.specific.SpecificExceptionBase"));
    assertTrue(errType.contains("public boolean catch$;"));

    assertTrue(protocol.contains("org.apache.avro.util.Utf8 goto$(org.apache.avro.util.Utf8 break$)"));
    assertTrue(protocol.contains("public interface default$"));
    assertTrue(protocol.contains("throws org.apache.avro.ipc.AvroRemoteException, finally$"));

  }

  @Test
  public void testManglingForRecords() {
    String schema = "" +
      "{ \"name\": \"volatile\", \"type\": \"record\", " +
      "  \"fields\": [ {\"name\": \"package\", \"type\": \"string\" }," +
      "                {\"name\": \"short\", \"type\": \"volatile\" } ] }";
    Collection<OutputFile> c =
      new SpecificCompiler(Schema.parse(schema)).compile();
    assertEquals(1, c.size());
    String contents = c.iterator().next().contents;

    assertTrue(contents.contains("public org.apache.avro.util.Utf8 package$;"));
    assertTrue(contents.contains("class volatile$ extends"));
    assertTrue(contents.contains("volatile$ short$;"));
  }

  @Test
  public void testManglingForEnums() {
    String enumSchema = "" +
      "{ \"name\": \"instanceof\", \"type\": \"enum\"," +
      "  \"symbols\": [\"new\", \"super\", \"switch\"] }";
    Collection<OutputFile> c =
      new SpecificCompiler(Schema.parse(enumSchema)).compile();
    assertEquals(1, c.size());
    String contents = c.iterator().next().contents;

    assertTrue(contents.contains("new$"));
  }

  /**
   * Called from TestSchema as part of its comprehensive checks.
   */
  public static Collection<OutputFile>
      compileWithSpecificCompiler(Schema schema) {
    return new SpecificCompiler(schema).compile();
  }
}
