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
package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;

import org.junit.Test;
import org.junit.Assert;

public class TestJsonDecoder {
  
  @Test public void testInt() throws Exception {
    checkNumeric("int", 1);
  }

  @Test public void testLong() throws Exception {
    checkNumeric("long", 1L);
  }

  @Test public void testFloat() throws Exception {
    checkNumeric("float", 1.0F);
  }

  @Test public void testDouble() throws Exception {
    checkNumeric("double", 1.0);
  }

  private void checkNumeric(String type, Object value) throws Exception {
    String def = 
      "{\"type\":\"record\",\"name\":\"X\",\"fields\":"
      +"[{\"type\":\""+type+"\",\"name\":\"n\"}]}";
    Schema schema = Schema.parse(def);
    DatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(schema);

    String[] records = {"{\"n\":1}", "{\"n\":1.0}"};

    for (String record : records) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, record);
      GenericRecord r = reader.read(null, decoder);
      Assert.assertEquals(value, r.get("n"));
    }
  }

}
