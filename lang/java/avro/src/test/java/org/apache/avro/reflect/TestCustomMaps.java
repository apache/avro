package org.apache.avro.reflect;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test serialization and de-serialization of classes extending Maps
 * 
 * @author sachingoyal
 */
@RunWith(Parameterized.class)
public class TestCustomMaps {

  private ReflectData rdata = null;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { ReflectData.AllowNull.get() },
        { ReflectData.get() },
    });
  }

  public TestCustomMaps(ReflectData rdata) {
    this.rdata = rdata;
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testSimpleStringKeyCustomMap() throws Exception {

    CustomHashMapRoot1 entityObj1 = buildCustomHashMapRoot1();

    String testType = "SimpleStringKeyCustomMap";
    CustomHashMapRoot1 [] entityObjs = {entityObj1};
    byte[] bytes = testReflectDataSerialization(testType, entityObj1);
    List<GenericRecord> genRecords =
        (List<GenericRecord>) testGenericDatumRead(testType, bytes, entityObjs);
    assertNotNull ("Unable to serialize and read field extending map", genRecords);

    GenericRecord genRecord = genRecords.get(0);
    assertNotNull ("No records in the deserialized data", genRecord);
    GenericRecord genMap = (GenericRecord) genRecord.get("map");
    assertNotNull ("Custom hash-map field could not be read", genMap);
    assertNotNull ("Unable to read generic field 'a'", genMap.get("a"));
    assertNotNull ("Unable to read generic field 'b'", genMap.get("b"));
    GenericRecord genAvroMapContainer =
        (GenericRecord) genMap.get(ReflectData.IMPLICIT_MAP_VIRTUAL_FIELD);
    String msg = "Unable to read generic field " + ReflectData.IMPLICIT_MAP_VIRTUAL_FIELD;
    assertNotNull (msg, genAvroMapContainer);
    HashMap genAmcMap =
        (HashMap) genAvroMapContainer.get(ReflectData.IMPLICIT_MAP_ACTUAL_FIELD);
    assertNotNull ("Unable to read generic field 'map'", genAmcMap);
    assertNotNull ("Unable to read generic field 'key'", genAmcMap.get(new Utf8("1")));
    assertNotNull ("Unable to read generic field 'value'", genAmcMap.get(new Utf8("2")));
    
    List<CustomHashMapRoot1> objs =
        testReflectDatumRead(testType, bytes, entityObjs);
    assertNotNull ("Unable to serialize and read field extending map", objs);
    CustomHashMapRoot1 record = objs.get(0);
    assertNotNull ("Unable to read object", record);
    assertNotNull ("Unable to read field 'map'", record.map);
    assertNotNull ("Unable to read field 'a'", record.map.a);
    assertNotNull ("Unable to read field 'b'", record.map.b);
    assertTrue ("Unable to read implicit map", record.map.entrySet().size() > 0);
    Iterator<Entry<String, BarValue1>> itr1 = entityObj1.map.entrySet().iterator();
    Iterator<Entry<String, BarValue1>> itr2 = record.map.entrySet().iterator();
    
    Entry<String, BarValue1> map1entry1 = itr1.next();
    Entry<String, BarValue1> map2entry1 = itr2.next();
    Entry<String, BarValue1> map2entry2 = itr2.next();
    assertTrue (map1entry1.getKey().equals(map2entry1.getKey()) ||
        map1entry1.getKey().equals(map2entry2.getKey()));
    assertTrue (map1entry1.getValue().name.equals(map2entry1.getValue().name) ||
        map1entry1.getValue().name.equals(map2entry2.getValue().name));

    byte[] jsonBytes = testJsonEncoder (testType, entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = testJsonDecoder(testType, jsonBytes, entityObj1);
    assertEquals ("JSON decoder output not same as Binary Decoder", 
      genRecord.get("map"), jsonRecord.get("map"));
  }

  public <T> byte[] testReflectDataSerialization(
      String testType, T ... entityObjs) throws Exception {

    log ("\n---- Beginning " + testType + " (" + rdata.getClass().getSimpleName() + ") ----");
    T entityObj1 = entityObjs[0];

    Schema schema = rdata.getSchema(entityObj1.getClass());
    assertNotNull("Unable to get schema for " + testType, schema);
    log (schema.toString(true));

    ReflectDatumWriter<T> datumWriter =
        new ReflectDatumWriter (entityObj1.getClass(), rdata);
    DataFileWriter<T> fileWriter = new DataFileWriter<T> (datumWriter);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    fileWriter.create(schema, baos);
    for (T entityObj : entityObjs) {
      fileWriter.append(entityObj);
    }
    fileWriter.close();

    byte[] bytes = baos.toByteArray();
    return bytes;
  }

  private <T> List<GenericRecord> testGenericDatumRead(
      String testType, byte[] bytes, T ... entityObjs) throws IOException {

    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord> ();
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<GenericRecord> fileReader =
        new DataFileReader<GenericRecord>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    assertNotNull("Unable to get schema for " + testType, schema);
    GenericRecord record = null;
    List<GenericRecord> records = new ArrayList<GenericRecord> ();
    while (fileReader.hasNext()) {
      records.add (fileReader.next(record));
    }
    return records;
  }

  private <T> List<T> testReflectDatumRead (
      String testType, byte[] bytes, T ... entityObjs) throws IOException {

    ReflectDatumReader<T> datumReader = new ReflectDatumReader<T> (rdata);
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<T> fileReader =
        new DataFileReader<T>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    T record = null;
    List<T> records = new ArrayList<T> ();
    while (fileReader.hasNext()) {
      records.add (fileReader.next(record));
    }
    return records;
  }

  private <T> byte[] testJsonEncoder
  (String testType, T entityObj) throws IOException {

    Schema schema = rdata.getSchema(entityObj.getClass());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<T>(schema, rdata);
    datumWriter.write(entityObj, encoder);
    encoder.flush();

    byte[] bytes = os.toByteArray();
    log ("\n---- JSON encoder output " + testType + " (" + rdata.getClass().getSimpleName() + "): ----\n" + new String(bytes));
    return bytes;
  }

  private <T> GenericRecord testJsonDecoder
  (String testType, byte[] bytes, T entityObj) throws IOException {

    Schema schema = rdata.getSchema(entityObj.getClass());
    GenericDatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>(schema);

    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes));
    GenericRecord r = datumReader.read(null, decoder);
    return r;
  }

  private void log(String s) {
    System.out.println (s);
  }

  private CustomHashMapRoot1 buildCustomHashMapRoot1 () {
    CustomHashMapRoot1 obj = new CustomHashMapRoot1();
    obj.map = new CustomHashMap1();
    obj.map.a = 1;
    obj.map.b = "Hello";
    
    BarValue1 v1 = new BarValue1();
    v1.name = "Hello";
    obj.map.put("1", v1);
    
    BarValue1 v2 = new BarValue1();
    v2.name = "Hello";
    obj.map.put("2", v2);
    
    return obj;
  }
}

//////////////////////////////////////////////////////////////////////////////////////
////// Temporary classes to test simple class deriving a HashMap with string-keys ////
//////////////////////////////////////////////////////////////////////////////////////
class CustomHashMapRoot1 {
  CustomHashMap1 map;
}

class BarValue1 {
  String name;
}

class CustomHashMap1 extends HashMap <String, BarValue1> {
  Integer a;
  String b;
}

