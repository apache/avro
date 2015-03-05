/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.reflect;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestPolymorphismWithAllowNull {

  @Test
  public void testPolymorphismWithAllowNull () throws Exception {
    ReflectData rdata = ReflectData.AllowNull.get();
    Schema schema = rdata.getSchema(PolymorphicDO.class);
    ReflectDatumWriter<PolymorphicDO> datumWriter = new ReflectDatumWriter (PolymorphicDO.class, rdata);
    DataFileWriter<PolymorphicDO> fileWriter = new DataFileWriter<PolymorphicDO> (datumWriter);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    fileWriter.create(schema, baos);
    PolymorphicDO pobj = new PolymorphicDO();
    fileWriter.append(pobj);
    fileWriter.close();
    byte[] bytes = baos.toByteArray();
    assertTrue ("Unable to serialize circular references",
        bytes.length > 0);

    // Test deserialization
    List<GenericRecord> records = testGenericDatumRead (bytes);
    assertEquals (1, records.size());
    GenericRecord record = records.get(0);
    Object obj = record.get("obj");
    assertNotNull (obj);
    assertEquals (new Integer(5), (Integer)((GenericRecord)obj).get("a"));
    assertNull (((GenericRecord)obj).get("b"));

    List<PolymorphicDO> objs = testReflectDatumRead (bytes);
    assertEquals (1, objs.size());
    pobj = objs.get(0);
    assertNotNull (pobj);
    assertEquals (new Integer(5), (Integer)pobj.obj.a);

    // Test JSON encoder/decoder
    byte[] jsonBytes = testJsonEncoder (pobj);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = testJsonDecoder(jsonBytes, pobj);
    assertEquals ("JSON decoder output not same as Binary Decoder", record, jsonRecord);
  }

  /**
   * Read with Generic Data
   */
  private <T> List<GenericRecord> testGenericDatumRead
    (byte[] bytes) throws IOException {

    GenericDatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord> ();
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<GenericRecord> fileReader =
      new DataFileReader<GenericRecord>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    GenericRecord record = null;
    List<GenericRecord> records = new ArrayList<GenericRecord> ();
    while (fileReader.hasNext()) {
      records.add (fileReader.next(record));
    }
    return records;
  }

  /**
   * Read with Reflect Data
   */
  private <T> List<T> testReflectDatumRead
    (byte[] bytes) throws IOException {

    ReflectDatumReader<T> datumReader = new ReflectDatumReader<T> ();
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<T> fileReader = new DataFileReader<T>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    T record = null;
    List<T> records = new ArrayList<T> ();
    while (fileReader.hasNext()) {
      records.add (fileReader.next(record));
    }
    return records;
  }

  /**
   * Write JSON
   */
  private <T> byte[] testJsonEncoder
    (T entityObj) throws IOException {

    ReflectData rdata = ReflectData.AllowNull.get();

    Schema schema = rdata.getSchema(entityObj.getClass());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<T>(schema, rdata);
    datumWriter.write(entityObj, encoder);
    encoder.flush();

    byte[] bytes = os.toByteArray();
    log ("JSON encoder output:\n" + new String(bytes));
    return bytes;
  }

  /**
   * Read JSON
   */
  private <T> GenericRecord testJsonDecoder
    (byte[] bytes, T entityObj) throws IOException {

    ReflectData rdata = ReflectData.AllowNull.get();

    Schema schema = rdata.getSchema(entityObj.getClass());
    GenericDatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord>(schema);

    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes));
    GenericRecord r = datumReader.read(null, decoder);
    return r;
  }

  private void log (String msg) {
    System.out.println (msg);
  }

  private static class Base 
  {
    Integer a = 5;
  }

  private static class Derived extends Base
  {
    String b = "Foo";
  }

  private static class PolymorphicDO
  {
    Base obj = new Derived();
  }
}
