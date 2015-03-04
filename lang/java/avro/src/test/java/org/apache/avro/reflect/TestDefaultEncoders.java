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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestDefaultEncoders {

  private ReflectData rdata = null;
  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { ReflectData.AllowNull.get() },
        { ReflectData.get() },
    });
  }

  public TestDefaultEncoders(ReflectData rdata) {
    this.rdata = rdata;
  }

  @Test
  public void testDateAndUuid() throws Exception {

    log("---- Running test with " + rdata.getClass().getSimpleName());
    DefaultEncoderTypes entityObj1 = buildDefaultEncoderTypes();
    DefaultEncoderTypes entityObj2 = buildDefaultEncoderTypes();

    List<DefaultEncoderTypes> records = testReflectData(entityObj1, entityObj2);
    assertEquals ("Unable to read all records", 2, records.size());
    DefaultEncoderTypes record = records.get(0);
    assertNotNull ("Unable to read field 'uuid'", record.uuid);
    assertNotNull ("Unable to read field 'date'", record.date);
    assertNotNull ("Unable to read field 'bd'", record.bd);
    
    assertTrue(entityObj1.uuid.equals(record.uuid) || 
        entityObj2.uuid.equals(record.uuid));
    
    byte[] jsonBytes = testJsonEncoder (entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    DefaultEncoderTypes jsonRecord = testJsonDecoder(jsonBytes, entityObj1);
    assertEquals ("JSON decoder output not same as Binary Decoder", 
        entityObj1.uuid, jsonRecord.uuid);
  }
  

  public <T> List<T> testReflectData (T ... entityObjs)
      throws Exception {

    T entityObj1 = entityObjs[0];
    Schema schema = rdata.getSchema(entityObj1.getClass());
    assertNotNull("Unable to get schema", schema);

    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter (entityObj1.getClass(), rdata);
    DataFileWriter<T> fileWriter = new DataFileWriter<T> (datumWriter);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    fileWriter.create(schema, baos);
    for (T entityObj : entityObjs) {
      fileWriter.append(entityObj);
    }
    fileWriter.close();

    byte[] bytes = baos.toByteArray();
    assertTrue ("Unable to serialize", bytes.length > 0);

    List<T> records = testReflectDataDeserialization(bytes, entityObjs);
    return records;
  }

  private <T> List<T> testReflectDataDeserialization
  (byte[] bytes, T ... entityObjs) throws IOException {
    String avroString = new String (bytes);
    log ("Avro binary: " + avroString);

    ReflectDatumReader<T> datumReader = new ReflectDatumReader<T> ();
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<T> fileReader = new DataFileReader<T>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    assertNotNull("Unable to get schema", schema);
    log ("--- Read schema successfully ----");
    List<T> records = new ArrayList<T> ();
    T record = null;
    int i=0;
    while (fileReader.hasNext()) {
      i++;
      record = fileReader.next(record);
      assertNotNull("Unable to read records", record);
      log ("Avro record " + i + ") " + record.toString());
      records.add(record);
    }
    assertEquals ("Unable to read same number of records as serialized", entityObjs.length, i);
    return records;
  }

  private <T> byte[] testJsonEncoder (T entityObj)
      throws IOException {

    Schema schema = rdata.getSchema(entityObj.getClass());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<T>(schema, rdata);
    datumWriter.write(entityObj, encoder);
    encoder.flush();

    byte[] bytes = os.toByteArray();
    System.out.println ("JSON encoder output:\n" + new String(bytes));
    return bytes;
  }
  
  private <T> DefaultEncoderTypes testJsonDecoder(byte[] bytes, T entityObj)
      throws IOException {

    Schema schema = rdata.getSchema(entityObj.getClass());
    ReflectDatumReader<DefaultEncoderTypes> datumReader = new ReflectDatumReader<DefaultEncoderTypes>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes));
    DefaultEncoderTypes r = datumReader.read(null, decoder);
    log (r.toString());
    return r;
  }


  private DefaultEncoderTypes buildDefaultEncoderTypes () {
    DefaultEncoderTypes obj = new DefaultEncoderTypes();
    if (rdata.getClass() != ReflectData.AllowNull.class) {
      obj.nullUuid = UUID.randomUUID();
      obj.nullDate = new Date();
      obj.nullBd = BigDecimal.TEN;
    }
    return obj;
  }

  private void log (String msg) {
    System.out.println (msg);
  }
}

class DefaultEncoderTypes {
  public UUID nullUuid = null;
  public UUID uuid = UUID.randomUUID();
  public Date nullDate = null;
  public Date date = new Date();
  public BigDecimal nullBd = null;
  public BigDecimal bd = BigDecimal.TEN;

  @Override
  public String toString() {
    return "DefaultEncoderTypes [uuid=" + uuid + ", date=" + date + ", bd=" + bd + "]";
  }
}
