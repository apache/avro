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

  // Ensure that even if the order of fields in JSON is different from the order in schema,
  // it works.
  @Test public void testReorderFields() throws Exception {
    String w =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":"
      +"[{\"type\":\"long\",\"name\":\"l\"},"
      +"{\"type\":{\"type\":\"array\",\"items\":\"int\"},\"name\":\"a\"}"
      +"]}";
    Schema ws = Schema.parse(w);
    DecoderFactory df = DecoderFactory.get();
    String data = "{\"a\":[1,2],\"l\":100}{\"l\": 200, \"a\":[1,2]}";
    JsonDecoder in = df.jsonDecoder(ws, data);
    Assert.assertEquals(100, in.readLong());
    in.skipArray();
    Assert.assertEquals(200, in.readLong());
    in.skipArray();
  }

  @Test public void testDecodingBase64() throws Exception {
    String schemaSrc = "{\"type\":\"record\",\"name\":\"SampleConfiguration\",\"namespace\":\"org.kaaproject.kaa.demo.configuration\",\"fields\":[{\"name\":\"AddressList\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Link\",\"fields\":[{\"name\":\"label\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"displayName\":\"Site label\"},{\"name\":\"url\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"displayName\":\"Site URL\"},{\"name\":\"__uuid\",\"type\":[{\"type\":\"fixed\",\"name\":\"uuidT\",\"namespace\":\"org.kaaproject.configuration\",\"size\":16},\"null\"],\"displayName\":\"Record Id\",\"fieldAccess\":\"read_only\"}],\"displayName\":\"Site address\"}},\"null\"],\"displayName\":\"URLs list\"},{\"name\":\"__uuid\",\"type\":[\"org.kaaproject.configuration.uuidT\",\"null\"],\"displayName\":\"Record Id\",\"fieldAccess\":\"read_only\"}]}";
    Schema schema = new Schema.Parser().parse(schemaSrc);
    String encoded = "{\"AddressList\":{\"array\":[{\"label\":\"Kaa website\",\"url\":\"http://www.kaaproject.org\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"4AGn74F5SeO7Zz/f8vUsFw==\"}},{\"label\":\"Kaa GitHub repository\",\"url\":\"https://github.com/kaaproject/kaa\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"fRoanhOjRMW7CGUWV2UI8A==\"}},{\"label\":\"Kaa docs\",\"url\":\"http://docs.kaaproject.org/display/KAA/Kaa+IoT+Platform+Home\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"olyL5ma7SpKTkRndP0r5Ig==\"}},{\"label\":\"Kaa configuration design reference\",\"url\":\"http://docs.kaaproject.org/display/KAA/Configuration\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"IdlwURhpTnmVU2qBp38kgg==\"}},{\"label\":\"Hello world\",\"url\":\"http://hello.world\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"OD8IP47QTzeXiuZ2nnw/mQ==\"}}]},\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"Suddjzq2QAKGPm5e/xDX2Q==\"}}";
    boolean base64 = true;
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, encoded, base64);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    GenericRecord record = datumReader.read(null, jsonDecoder);
    String actual = record.toString();
    String expected = "{\"AddressList\": [{\"label\": \"Kaa website\", \"url\": \"http://www.kaaproject.org\", \"__uuid\": [-32, 1, -89, -17, -127, 121, 73, -29, -69, 103, 63, -33, -14, -11, 44, 23]}, {\"label\": \"Kaa GitHub repository\", \"url\": \"https://github.com/kaaproject/kaa\", \"__uuid\": [125, 26, 26, -98, 19, -93, 68, -59, -69, 8, 101, 22, 87, 101, 8, -16]}, {\"label\": \"Kaa docs\", \"url\": \"http://docs.kaaproject.org/display/KAA/Kaa+IoT+Platform+Home\", \"__uuid\": [-94, 92, -117, -26, 102, -69, 74, -110, -109, -111, 25, -35, 63, 74, -7, 34]}, {\"label\": \"Kaa configuration design reference\", \"url\": \"http://docs.kaaproject.org/display/KAA/Configuration\", \"__uuid\": [33, -39, 112, 81, 24, 105, 78, 121, -107, 83, 106, -127, -89, 127, 36, -126]}, {\"label\": \"Hello world\", \"url\": \"http://hello.world\", \"__uuid\": [56, 63, 8, 63, -114, -48, 79, 55, -105, -118, -26, 118, -98, 124, 63, -103]}], \"__uuid\": [74, -25, 93, -113, 58, -74, 64, 2, -122, 62, 110, 94, -1, 16, -41, -39]}";
    Assert.assertNotNull("Resulting string shouldn't be null", actual);
    Assert.assertEquals("Actual result of encoding is not equal to expected", expected, actual);
  }
}
