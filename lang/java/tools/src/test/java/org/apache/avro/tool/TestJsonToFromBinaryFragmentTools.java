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
import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;

/**
 * Tests both {@link JsonToBinaryFragmentTool} 
 * and {@link BinaryFragmentToJsonTool}.
 */
public class TestJsonToFromBinaryFragmentTools {
  private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);
  private static final String UTF8 = "utf-8";
  private static final String AVRO = 
    "ZLong string implies readable length encoding.";
  private static final String JSON = 
    "\"Long string implies readable length encoding.\"\n";

  @Test
  public void testBinaryToJson() throws Exception {
    binaryToJson(AVRO, JSON);
  }
  
  @Test
    public void testJsonToBinary() throws Exception {
    jsonToBinary(JSON, AVRO);
  }

  @Test
    public void testMultiBinaryToJson() throws Exception {
    binaryToJson(AVRO + AVRO + AVRO, JSON + JSON + JSON);
  }

  @Test
    public void testMultiJsonToBinary() throws Exception {
    jsonToBinary(JSON + JSON + JSON, AVRO + AVRO + AVRO);
  }

  private void binaryToJson(String avro, String json) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(new BufferedOutputStream(baos));
    
    new BinaryFragmentToJsonTool().run(
        new ByteArrayInputStream(avro.getBytes(UTF8)), // stdin
        p, // stdout
        null, // stderr
        Arrays.asList(STRING_SCHEMA.toString(), "-"));
    assertEquals(json, baos.toString(UTF8).replace("\r", ""));
  }
  
  private void jsonToBinary(String json, String avro) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(new BufferedOutputStream(baos));

    new JsonToBinaryFragmentTool().run(
        new ByteArrayInputStream(json.getBytes(UTF8)), // stdin
        p, // stdout
        null, // stderr
        Arrays.asList(STRING_SCHEMA.toString(), "-"));
    assertEquals(avro, baos.toString(UTF8));
  }
}
