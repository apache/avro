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
package org.apache.avro.tool;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.junit.Test;

/**
 * Tests both {@link JsonToBinaryFragmentTool} 
 * and {@link BinaryFragmentToJsonTool}.
 */
public class TestJsonToFromBinaryFragmentTools {
  private static final Schema STRING_SCHEMA = Schema.parse("\"string\"");
  private static final String UTF8 = "utf-8";
  private static final String AVRO = 
    "ZLong string implies readable length encoding.";
  private static final String JSON = 
    "\"Long string implies readable length encoding.\"";

  @Test
  public void testBinaryToJson() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(baos);
    
    new BinaryFragmentToJsonTool().run(
        new ByteArrayInputStream(AVRO.getBytes(UTF8)), // stdin
        p, // stdout
        null, // stderr
        Arrays.asList(STRING_SCHEMA.toString(), "-"));
    assertEquals(JSON + "\n", baos.toString(UTF8).replace("\r", ""));
  }
  
  @Test
  public void testJsonToBinary() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(baos);
    new JsonToBinaryFragmentTool().run(
        new ByteArrayInputStream(JSON.getBytes(UTF8)), // stdin
        p, // stdout
        null, // stderr
        Arrays.asList(STRING_SCHEMA.toString(), "-"));
    assertEquals(AVRO, baos.toString(UTF8));
  }
}
