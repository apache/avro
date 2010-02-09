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
package org.apache.avro.generic;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema.Type;

import org.junit.Test;

public class TestGenericData {
  
  @Test(expected=AvroRuntimeException.class)
    public void testrecordConstructorNullSchema() throws Exception {
    new GenericData.Record(null);
  }
    
  @Test(expected=AvroRuntimeException.class)
    public void testrecordConstructorWrongSchema() throws Exception {
    new GenericData.Record(Schema.create(Schema.Type.INT));
  }

  @Test(expected=AvroRuntimeException.class)
    public void testArrayConstructorNullSchema() throws Exception {
    new GenericData.Array<Object>(1, null);
  }
    
  @Test(expected=AvroRuntimeException.class)
    public void testArrayConstructorWrongSchema() throws Exception {
    new GenericData.Array<Object>(1, Schema.create(Schema.Type.INT));
  }
  
  @Test
  /** Make sure that even with nulls, hashCode() doesn't throw NPE. */
  public void testHashCode() {
    GenericData.get().hashCode(null, Schema.create(Type.NULL));
    GenericData.get().hashCode(null, Schema.createUnion(
        Arrays.asList(Schema.create(Type.BOOLEAN), Schema.create(Type.STRING))));
  }

  @Test
  public void testRecordGetFieldDoesntExist() throws Exception {
    List<Field> fields = new ArrayList<Field>();
    Schema schema = Schema.createRecord(fields);
    GenericData.Record record = new GenericData.Record(schema);
    assertNull(record.get("does not exist"));
  }
    
}
