/*
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
package org.apache.avro.io.parsing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestUnionPromotion {

  @Test
  public void testUnionPromotionCollection() throws Exception {
    Schema directFieldSchema = SchemaBuilder.record("MyRecord").namespace("ns").fields().name("field1").type().map()
        .values().stringType().noDefault().endRecord();
    Schema schemaWithField = SchemaBuilder.record("MyRecord").namespace("ns").fields().name("field1").type().nullable()
        .map().values().stringType().noDefault().endRecord();
    Map<String, String> data = new HashMap<>();
    data.put("a", "someValue");
    GenericData.Record record = new GenericRecordBuilder(directFieldSchema).set("field1", data).build();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    writeAvroBin(bos, record);
    Object read = readAvroBin(new ByteArrayInputStream(bos.toByteArray()), directFieldSchema, schemaWithField);
    Map name = (Map) ((GenericRecord) read).get("field1");
    Assert.assertEquals("someValue", name.get(new Utf8("a")).toString());

  }

  private static Object readAvroBin(final InputStream input, final Schema writerSchema, final Schema readerSchema)
      throws IOException {
    DatumReader reader = new GenericDatumReader(writerSchema, readerSchema);
    DecoderFactory decoderFactory = DecoderFactory.get();
    Decoder decoder = decoderFactory.binaryDecoder(input, null);
    return reader.read(null, decoder);
  }

  private static void writeAvroBin(final OutputStream out, final GenericRecord req) throws IOException {
    @SuppressWarnings("unchecked")
    DatumWriter writer = new GenericDatumWriter(req.getSchema());
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(req, encoder);
    encoder.flush();
  }

}
