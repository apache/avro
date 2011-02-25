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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.junit.Test;

public class TestGenericDatumWriter {
  @Test
  public void testWrite() throws IOException {
    String json = "{\"type\": \"record\", \"name\": \"r\", \"fields\": ["
      + "{ \"name\": \"f1\", \"type\": \"long\" }"
      + "]}";
    Schema s = Schema.parse(json);
    GenericRecord r = new GenericData.Record(s);
    r.put("f1", 100L);
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> w =
      new GenericDatumWriter<GenericRecord>(s);
    Encoder e = EncoderFactory.get().jsonEncoder(s, bao);
    w.write(r, e);
    e.flush();
    
    Object o = new GenericDatumReader<GenericRecord>(s).read(null,
        DecoderFactory.get().jsonDecoder(s, new ByteArrayInputStream(bao.toByteArray())));
    assertEquals(r, o);
  }
}
