/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;

import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class TestSpecificRecordWithUnion {
  @Test
  public void testUnionLogicalDecimalConversion() throws IOException {
    final TestUnionRecord record = TestUnionRecord.newBuilder().setAmount(BigDecimal.ZERO).build();
    final Schema schema = SchemaBuilder.unionOf().nullType().and().type(record.getSchema()).endUnion();

    byte[] recordBytes = serializeRecord(
        "{ \"org.apache.avro.specific.TestUnionRecord\": { \"amount\": { \"bytes\": \"\\u0000\" } } }", schema);

    SpecificDatumReader<SpecificRecord> specificDatumReader = new SpecificDatumReader<>(schema);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(recordBytes);
    Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
    final SpecificRecord deserialized = specificDatumReader.read(null, decoder);
    assertEquals(record, deserialized);
  }

  public static byte[] serializeRecord(String value, Schema schema) throws IOException {
    DatumReader<Object> reader = new GenericDatumReader<>(schema);
    Object object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, value));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
    writer.write(object, encoder);
    encoder.flush();
    byte[] bytes = out.toByteArray();
    out.close();
    return bytes;
  }
}
