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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.TestProtocolParsing;
import org.apache.avro.TestSchema;
import org.junit.Test;

public class TestTemplatedCompilerFidelity {
  SpecificCompiler templated = new SpecificCompiler();
  Avro14SpecificCompiler legacy = new Avro14SpecificCompiler();
  /** If true, records output to /tmp, for diffing with real tools. */
  private final static boolean DEBUG = true;

  @Test
  public void testSchemas() throws IOException {
    Schema[] schemas = new Schema[] {
        Schema.parse(TestSchema.BASIC_ENUM_SCHEMA),
        Schema.parse(TestSchema.SCHEMA_WITH_DOC_TAGS),
        Schema.parse(TestSchema.SCHEMA_WITH_DOC_TAGS).getField("inner_fixed").schema(),
        Schema.parse(TestSchema.SCHEMA_WITH_DOC_TAGS).getField("inner_enum").schema(),
        Schema.parse(TestSchema.LISP_SCHEMA),
        // The error schema in the following protocol
        Protocol.parse(TestSpecificCompiler.PROTOCOL).getMessages().get("goto").getErrors().getTypes().get(1)
    };
    for (Schema s : schemas) {
      String expected = legacy.compile(s).contents;
      String actual = templated.compile(s).contents;
      debug(expected, actual);
      assertEquals(expected, actual);
    }
    
  }
  
  @Test
  public void testProtocols() throws IOException {
    Protocol p = TestProtocolParsing.getSimpleProtocol();
    String expected = legacy.compileInterface(p).contents;
    String actual = templated.compileInterface(p).contents;
    debug(expected, actual);
    assertEquals(expected, actual);
  }

  /** Because asserts are pretty poor for debugging, optionally write to
   * temporary files. 
   */
  @SuppressWarnings("unused")
  private void debug(String expected, String actual) throws IOException {
    if (expected.equals(actual)) return;
    
    if (DEBUG) {
      writeToFile(new File("/tmp/a"), expected);
      writeToFile(new File("/tmp/b"), actual);
    }
  }

  private void writeToFile(File file, String expected) throws IOException {
    FileWriter fw = new FileWriter(file);
    fw.write(expected);
    fw.close();
  }
}
