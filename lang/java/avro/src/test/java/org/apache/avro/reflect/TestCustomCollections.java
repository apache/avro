package org.apache.avro.reflect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

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
 * Test serialization and de-serialization of classes extending Collections
 * 
 * @author sachingoyal
 */
@RunWith(Parameterized.class)
public class TestCustomCollections {

  private ReflectData rdata = null;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { ReflectData.AllowNull.get() },
        { ReflectData.get() },
    });
  }

  public TestCustomCollections(ReflectData rdata) {
    this.rdata = rdata;
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testSimpleCustomCollection() throws Exception {

    CustomListRoot1 entityObj1 = buildCustomListRoot1();

    String testType = "SimpleStringKeyCustomMap";
    CustomListRoot1 [] entityObjs = {entityObj1};
    byte[] bytes = testCircularSerialization(testType, entityObj1);
    List<GenericRecord> genRecords =
        (List<GenericRecord>) testGenericDatumRead(testType, bytes, entityObjs);
    assertNotNull ("Unable to serialize and read field extending map", genRecords);

    GenericRecord genRecord = genRecords.get(0);
    log (genRecord.toString());
    assertNotNull ("No records in the deserialized data", genRecord);
    GenericRecord genCollection = (GenericRecord) genRecord.get("customList");
    assertNotNull ("Custom collection field could not be read", genCollection);
    assertNotNull ("Unable to read generic field 'a'", genCollection.get("a"));
    assertNotNull ("Unable to read generic field 'b'", genCollection.get("b"));
    GenericRecord genAvroCollectionContainer =
        (GenericRecord) genCollection.get(ReflectData.IMPLICIT_COLLECTION_VIRTUAL_FIELD);
    String msg = "Unable to read generic field " + ReflectData.IMPLICIT_COLLECTION_VIRTUAL_FIELD;
    assertNotNull (msg, genAvroCollectionContainer);
    GenericArray genCustomList =
        (GenericArray) genAvroCollectionContainer.get(ReflectData.IMPLICIT_COLLECTION_ACTUAL_FIELD);
    assertNotNull ("Unable to read generic field 'customList'", genCustomList);
    assertNotNull ("Unable to read generic field 'key'", genCustomList.get(0));
    assertNotNull ("Unable to read generic field 'value'", genCustomList.get(1));
    
    List<CustomListRoot1> objs =
        testReflectDatumRead(testType, bytes, entityObjs);
    assertNotNull ("Unable to serialize and read field extending map", objs);
    CustomListRoot1 record = objs.get(0);
    assertNotNull ("Unable to read object", record);
    assertNotNull ("Unable to read field 'customList'", record.customList);
    assertNotNull ("Unable to read field 'a'", record.customList.a);
    assertNotNull ("Unable to read field 'b'", record.customList.b);
    assertTrue ("Unable to read implicit collection", record.customList.size() > 0);

    assertEquals (entityObj1.customList.size(), record.customList.size());
    Iterator<Node1> itr1 = entityObj1.customList.iterator();
    Iterator<Node1> itr2 = record.customList.iterator();
    
    Node1 list1entry1 = itr1.next();
    Node1 list1entry2 = itr1.next();
    Node1 list2entry1 = itr2.next();
    Node1 list2entry2 = itr2.next();
    assertEquals (list1entry1.name, list2entry1.name);
    assertEquals (list1entry2.name, list2entry2.name);

    byte[] jsonBytes = testJsonEncoder (testType, entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = testJsonDecoder(testType, jsonBytes, entityObj1);
    assertEquals ("JSON decoder output not same as Binary Decoder", 
      genRecord.get("customList"), jsonRecord.get("customList"));
  }

  public <T> byte[] testCircularSerialization(
      String testType, T ... entityObjs) throws Exception {

    log ("\n---- Beginning " + testType + " (" + rdata.getClass().getSimpleName() + ") ----");
    T entityObj1 = entityObjs[0];

    Schema schema = rdata.getSchema(entityObj1.getClass());
    assertNotNull("Unable to get schema for circular reference in " + testType, schema);
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

  private CustomListRoot1 buildCustomListRoot1 () {
    CustomListRoot1 obj = new CustomListRoot1();
    obj.customList = new CustomList1();
    obj.customList.a = 1;
    obj.customList.b = "Hello";
    
    Node1 n = new Node1();
    n.name = "foo";
    obj.customList.add(n);
    
    n = new Node1();
    n.name = "bar";
    obj.customList.add(n);
    
    return obj;
  }
}

/////////////////////////////////////////////////////////////////
////// Temporary classes to test class deriving a Collection ////
/////////////////////////////////////////////////////////////////
class CustomListRoot1 {
  CustomList1 customList;
}

class Node1 {
  String name;
}

class CustomList1 extends ArrayList <Node1> {
  Integer a;
  String b;
}
