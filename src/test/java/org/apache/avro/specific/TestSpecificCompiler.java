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

  /**
   * Called from TestSchema as part of its comprehensive checks.
   */
  public static Collection<OutputFile>
      compileWithSpecificCompiler(Schema schema) {
    return new SpecificCompiler(schema).compile();
  }
}
