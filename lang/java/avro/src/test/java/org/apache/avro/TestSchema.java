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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class TestSchema {  
  @Test
  public void testSplitSchemaBuild() {
    Schema s = SchemaBuilder
       .record("HandshakeRequest")
       .namespace("org.apache.avro.ipc").fields()
         .name("clientProtocol").type().optional().stringType()
         .name("meta").type().optional().map().values().bytesType()
         .endRecord();
    
    String schemaString = s.toString();
    final int mid = schemaString.length() / 2;
    
    Schema parsedStringSchema = new org.apache.avro.Schema.Parser().parse(s.toString());
    Schema parsedArrayOfStringSchema =
      new org.apache.avro.Schema.Parser().parse
      (schemaString.substring(0, mid), schemaString.substring(mid));
    assertNotNull(parsedStringSchema);
    assertNotNull(parsedArrayOfStringSchema);
    assertEquals(parsedStringSchema.toString(), parsedArrayOfStringSchema.toString());
  }
}
