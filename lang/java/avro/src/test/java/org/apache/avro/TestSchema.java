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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
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

  @Test
  public void testDuplicateRecordFieldName() {
    final Schema schema = Schema.createRecord("RecordName", null, null, false);
    final List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("field_name", Schema.create(Type.NULL), null, null));
    fields.add(new Field("field_name", Schema.create(Type.INT), null, null));
    try {
      schema.setFields(fields);
      fail("Should not be able to create a record with duplicate field name.");
    } catch (AvroRuntimeException are) {
      assertTrue(are.getMessage().contains("Duplicate field field_name in record RecordName"));
    }
  }

  @Test
  public void testCreateUnionVarargs() {
    List<Schema> types = new ArrayList<Schema>();
    types.add(Schema.create(Type.NULL));
    types.add(Schema.create(Type.LONG));
    Schema expected = Schema.createUnion(types);

    Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
    assertEquals(expected, schema);
  }
}
