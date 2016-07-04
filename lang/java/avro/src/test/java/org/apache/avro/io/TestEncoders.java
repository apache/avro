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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Assert;
import org.junit.Test;

public class TestEncoders {
  private static EncoderFactory factory = EncoderFactory.get();

  @Test
  public void testBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = factory.binaryEncoder(out, null);
    Assert.assertTrue(enc == factory.binaryEncoder(out, enc));
  }

  @Test(expected=NullPointerException.class)
  public void testBadBinaryEncoderInit() {
    factory.binaryEncoder(null, null);
  }

  @Test
  public void testBlockingBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder reuse = null;
    reuse = factory.blockingBinaryEncoder(out, reuse);
    Assert.assertTrue(reuse == factory.blockingBinaryEncoder(out, reuse));
    // comparison
  }

  @Test(expected=NullPointerException.class)
  public void testBadBlockintBinaryEncoderInit() {
    factory.binaryEncoder(null, null);
  }

  @Test
  public void testDirectBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = factory.directBinaryEncoder(out, null);
    Assert.assertTrue(enc ==  factory.directBinaryEncoder(out, enc));
  }

  @Test(expected=NullPointerException.class)
  public void testBadDirectBinaryEncoderInit() {
    factory.directBinaryEncoder(null, null);
  }

  @Test
  public void testJsonEncoderInit() throws IOException {
    Schema s = Schema.parse("\"int\"");
    OutputStream out = new ByteArrayOutputStream();
    factory.jsonEncoder(s, out);
    JsonEncoder enc = factory.jsonEncoder(s,
        new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8));
    enc.configure(out);
  }

  @Test(expected=NullPointerException.class)
  public void testBadJsonEncoderInitOS() throws IOException {
    factory.jsonEncoder(Schema.create(Type.INT), (OutputStream)null);
  }

  @Test(expected=NullPointerException.class)
  public void testBadJsonEncoderInit() throws IOException {
    factory.jsonEncoder(Schema.create(Type.INT), (JsonGenerator)null);
  }

  @Test
  public void testJsonEncoderNewlineDelimited() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    Schema ints = Schema.create(Type.INT);
    Encoder e = factory.jsonEncoder(ints, out);
    String separator = System.getProperty("line.separator");
    GenericDatumWriter<Integer> writer = new GenericDatumWriter<Integer>(ints);
    writer.write(1, e);
    writer.write(2, e);
    e.flush();
    Assert.assertEquals("1"+separator+"2", out.toString());
  }

  @Test
  public void testValidatingEncoderInit() throws IOException {
    Schema s = Schema.parse("\"int\"");
    OutputStream out = new ByteArrayOutputStream();
    Encoder e = factory.directBinaryEncoder(out, null);
    factory.validatingEncoder(s, e).configure(e);
  }

  @Test
  public void testJsonRecordOrdering() throws IOException {
    String value = "{\"b\": 2, \"a\": 1}";
    Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
        "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"int\"}" +
        "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, value);
    Object o = reader.read(null, decoder);
    Assert.assertEquals("{\"a\": 1, \"b\": 2}", o.toString());
  }

  @Test(expected=AvroTypeException.class)
  public void testJsonExcessFields() throws IOException {
    String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a0\": 45, \"a2\":true, \"a1\": null}}";
    Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
        "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
        "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n" +
        "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n" +
        "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n" +
        "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, value);
    reader.read(null, decoder);
  }

  @Test
  public void testJsonRecordOrdering2() throws IOException {
    String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
    Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
        "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
        "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n" +
        "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n" +
        "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n" +
        "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, value);
    Object o = reader.read(null, decoder);
    Assert.assertEquals("{\"a\": {\"a1\": null, \"a2\": true}, \"b\": {\"b1\": \"h\", \"b2\": 3.14, \"b3\": 1.4}}", o.toString());
  }

  @Test
  public void testJsonRecordOrderingWithProjection() throws IOException {
    String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
    Schema writerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
        "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
        "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n" +
        "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n" +
        "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n" +
        "]}");
    Schema readerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
      "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
      "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" +
      "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(writerSchema, readerSchema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, value);
    Object o = reader.read(null, decoder);
    Assert.assertEquals("{\"a\": {\"a1\": null, \"a2\": true}}", o.toString());
  }

  @Test
  public void testJsonRecordOrderingWithProjection2() throws IOException {
    String value = "{\"b\": { \"b1\": \"h\", \"b2\": [3.14, 3.56], \"b3\": 1.4}, \"a\": {\"a2\":true, \"a1\": null}}";
    Schema writerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
        "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
        "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n" +
        "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n" +
        "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":{\"type\":\"array\", \"items\":\"float\"}}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n" +
        "]}");
    Schema readerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
      "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
      "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" +
      "]}");
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(writerSchema, readerSchema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, value);
    Object o = reader.read(null, decoder);
    Assert.assertEquals("{\"a\": {\"a1\": null, \"a2\": true}}", o.toString());
  }

  @Test public void testJsonEncodingBase64() throws Exception {
    String schemaSrc = "{\"type\":\"record\",\"name\":\"SampleConfiguration\",\"namespace\":\"org.kaaproject.kaa.demo.configuration\",\"fields\":[{\"name\":\"AddressList\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Link\",\"fields\":[{\"name\":\"label\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"displayName\":\"Site label\"},{\"name\":\"url\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"displayName\":\"Site URL\"},{\"name\":\"__uuid\",\"type\":[{\"type\":\"fixed\",\"name\":\"uuidT\",\"namespace\":\"org.kaaproject.configuration\",\"size\":16},\"null\"],\"displayName\":\"Record Id\",\"fieldAccess\":\"read_only\"}],\"displayName\":\"Site address\"}},\"null\"],\"displayName\":\"URLs list\"},{\"name\":\"__uuid\",\"type\":[\"org.kaaproject.configuration.uuidT\",\"null\"],\"displayName\":\"Record Id\",\"fieldAccess\":\"read_only\"}]}";
    Schema schema = new Schema.Parser().parse(schemaSrc);
    //getting record to encode
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    String recordWithLatin1 = "{\"AddressList\":{\"array\":[{\"label\":\"Kaa website\",\"url\":\"http://www.kaaproject.org\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"à\\u0001§ï\u0081yIã»g?ßòõ,\\u0017\"}},{\"label\":\"Kaa GitHub repository\",\"url\":\"https://github.com/kaaproject/kaa\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"}\\u001A\\u001A\u009E\\u0013£DÅ»\\be\\u0016We\\bð\"}},{\"label\":\"Kaa docs\",\"url\":\"http://docs.kaaproject.org/display/KAA/Kaa+IoT+Platform+Home\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"¢\\\\\u008Bæf»J\u0092\u0093\u0091\\u0019ÝŽJù\\\"\"}},{\"label\":\"Kaa configuration design reference\",\"url\":\"http://docs.kaaproject.org/display/KAA/Configuration\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"!ÙpQ\\u0018iNy\u0095Sj\u0081§\u007F$\u0082\"}},{\"label\":\"Hello world\",\"url\":\"http://hello.world\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"8Ž\\bŸ\u008EÐO7\u0097\u008Aæv\u009E|Ž\u0099\"}}]},\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"Jç]\u008F:¶@\\u0002\u0086>n^ÿ\\u0010×Ù\"}}";
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, recordWithLatin1);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    GenericRecord record = datumReader.read(null, jsonDecoder);
    //encoding
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(true, schema, baos);
    datumWriter.write(record, jsonEncoder);
    jsonEncoder.flush();
    baos.flush();
    byte[] bytes = baos.toByteArray();
    String actual = new String(bytes);

    String expected = "{\"AddressList\":{\"array\":[{\"label\":\"Kaa website\",\"url\":\"http://www.kaaproject.org\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"4AGn74F5SeO7Zz/f8vUsFw==\"}},{\"label\":\"Kaa GitHub repository\",\"url\":\"https://github.com/kaaproject/kaa\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"fRoanhOjRMW7CGUWV2UI8A==\"}},{\"label\":\"Kaa docs\",\"url\":\"http://docs.kaaproject.org/display/KAA/Kaa+IoT+Platform+Home\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"olyL5ma7SpKTkRndP0r5Ig==\"}},{\"label\":\"Kaa configuration design reference\",\"url\":\"http://docs.kaaproject.org/display/KAA/Configuration\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"IdlwURhpTnmVU2qBp38kgg==\"}},{\"label\":\"Hello world\",\"url\":\"http://hello.world\",\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"OD8IP47QTzeXiuZ2nnw/mQ==\"}}]},\"__uuid\":{\"org.kaaproject.configuration.uuidT\":\"Suddjzq2QAKGPm5e/xDX2Q==\"}}";
    Assert.assertNotNull("Resulting string shouldn't be null", actual);
    Assert.assertEquals("Actual result of encoding is not equal to expected", expected, actual);
  }

}
