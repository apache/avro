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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * Test serialization and de-serialization for Java polymorphism
 * For example, when a field of type base-class holds a derived-class object.
 */
public class TestPolymorphicSetSchema {

  private boolean applyFieldSchema = true; 

  @Test
  public void testFieldAndClassLevelSchema() throws Exception {
    applyFieldSchema = true;
    testPolymorphicFields("testFieldLevelSchema");
    applyFieldSchema = false;
    testPolymorphicFields("testClassLevelSchema");
  }

  public void testPolymorphicFields(String testType) throws Exception {

    Showroom entityObj1 = buildShowroom();
    Showroom entityObj2 = buildShowroom();

    Showroom [] entityObjs = {entityObj1, entityObj2};
    byte[] bytes = testSerialization(testType, entityObj1, entityObj2);
    List<GenericRecord> records = 
      (List<GenericRecord>) testGenericDatumRead(testType, bytes, entityObjs);

    GenericRecord record = records.get(0);
    Object trending = record.get("trending");
    assertTrue ("Unable to read 'trending' list", trending instanceof GenericArray);

    GenericArray arrayTrending = ((GenericArray)trending);

    // Test chair specific properties
    Object chair = arrayTrending.get(0);
    assertTrue (chair instanceof GenericRecord);
    Object color = ((GenericRecord)chair).get("color");
    Object height = ((GenericRecord)chair).get("height");
    Object percentDiscount = ((GenericRecord)chair).get("percentDiscount");
    assertNotNull (color);
    assertNull (height);
    assertNull (percentDiscount);

    // Test table specific properties
    Object table = arrayTrending.get(1);
    assertTrue (table instanceof GenericRecord);
    color = ((GenericRecord)table).get("color");
    height = ((GenericRecord)table).get("height");
    percentDiscount = ((GenericRecord)table).get("percentDiscount");
    assertNull (color);
    assertNotNull (height);
    assertNull (percentDiscount);

    // Test swivel-chair specific properties
    Object swivelChair = arrayTrending.get(2);
    assertTrue (swivelChair instanceof GenericRecord);
    color = ((GenericRecord)swivelChair).get("color");
    height = ((GenericRecord)swivelChair).get("height");
    percentDiscount = ((GenericRecord)swivelChair).get("percentDiscount");
    assertNotNull (color); // because swivel is also a chair
    assertNull (height);
    assertNotNull (percentDiscount);

    // Test reflect data
    List<Showroom> records2 =
      (List<Showroom>) testReflectDatumRead(testType, bytes, entityObjs);
    Showroom showroom = records2.get(0);
    log ("Read: " + showroom);
    List<Item> trendingItems = showroom.getTrending();
    assertNotNull (trendingItems);
    assertEquals (3, trendingItems.size());

    Item item1 = trendingItems.get(0);
    Item item2 = trendingItems.get(1);
    Item item3 = trendingItems.get(2);
    assertTrue (item1 instanceof Chair);
    assertTrue (item2 instanceof Table);
    assertTrue (item3 instanceof SwivelChair);

    assertTrue (showroom.getMostSelling() instanceof Chair);
    assertTrue (showroom.getLeastSelling() instanceof SwivelChair);

    // Test json encoder/decoder
    byte[] jsonBytes = testJsonEncoder (testType, entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = testJsonDecoder(testType, jsonBytes, entityObj1);
    assertEquals ("JSON decoder output not same as Binary Decoder", record, jsonRecord);
  }

  private ReflectData getReflectData () {
    if (applyFieldSchema)
      return getReflectDataWithFieldLevelSchema();
    else
      return getReflectDataWithClassLevelSchema();
  }

  private ReflectData getReflectDataWithClassLevelSchema () {
    ReflectData rdata = ReflectData.AllowNull.get();
 
    // Get schemas for all hierarchies
    Schema chairSchema = rdata.getSchema(Chair.class);
    Schema tableSchema = rdata.getSchema(Table.class);
    Schema swivelSchema = rdata.getSchema(SwivelChair.class);

    // Since the list can contain any type of derived classes,
    // we create a union for all of the possible types here.
    // And then we create an array of the union and make it nullable
    List<Schema> unionTypes = new ArrayList<Schema> (2);
    unionTypes.add(chairSchema);
    unionTypes.add(tableSchema);
    unionTypes.add(swivelSchema);
    Schema unionSchema = Schema.createUnion(unionTypes);

    try {
      rdata.setSchema (Item.class, unionSchema);
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    return rdata;
  }

  private ReflectData getReflectDataWithFieldLevelSchema () {
    ReflectData rdata = ReflectData.AllowNull.get();
 
    // Get schemas for all hierarchies
    Schema chairSchema = rdata.getSchema(Chair.class);
    Schema tableSchema = rdata.getSchema(Table.class);
    Schema swivelSchema = rdata.getSchema(SwivelChair.class);

    // Since the list can contain any type of derived classes,
    // we create a union for all of the possible types here.
    // And then we create an array of the union and make it nullable
    List<Schema> unionTypes = new ArrayList<Schema> (2);
    unionTypes.add(chairSchema);
    unionTypes.add(tableSchema);
    unionTypes.add(swivelSchema);
    Schema unionSchema = Schema.createUnion(unionTypes);
    Schema listSchema = Schema.createArray (unionSchema);
    Schema nullableListSchema = rdata.makeNullable (listSchema);

    try {
      // Get maverick fields
      Field mostSelling = Showroom.class.getDeclaredField("mostSelling");
      Field leastSelling = Showroom.class.getDeclaredField("leastSelling");
      Field trending = Showroom.class.getDeclaredField("trending");

      // Set the schema for each of the fields
      rdata.setSchema (mostSelling, chairSchema);
      rdata.setSchema (leastSelling, swivelSchema);
      rdata.setSchema (trending, nullableListSchema);
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
    return rdata;
  }
  
  /**
   * Test serialization of non-string map-key POJOs
   */
  public <T> byte[] testSerialization(String testType, T ... entityObjs) throws Exception {

    log ("---- Beginning " + testType + " ----");
    T entityObj1 = entityObjs[0];
    ReflectData rdata = getReflectData();

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

  /**
   * Test that non-string map-keys are readable through GenericDatumReader
   * This methoud should read as array of {key, value} and not as a map
   */
  private <T> List<GenericRecord> testGenericDatumRead
    (String testType, byte[] bytes, T ... entityObjs) throws IOException {

    GenericDatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord> ();
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

  /**
   * Test that non-string map-keys are readable through ReflectDatumReader
   * This methoud should form the original map and should not return any
   * array of {key, value} as done by {@link #testGenericDatumRead()} 
   */
  private <T> List<T> testReflectDatumRead
    (String testType, byte[] bytes, T ... entityObjs) throws IOException {

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

  private <T> byte[] testJsonEncoder
    (String testType, T entityObj) throws IOException {

    ReflectData rdata = getReflectData();

    Schema schema = rdata.getSchema(entityObj.getClass());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder(schema, os);
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<T>(schema, rdata);
    datumWriter.write(entityObj, encoder);
    encoder.flush();

    byte[] bytes = os.toByteArray();
    System.out.println ("JSON encoder output:\n" + new String(bytes));
    return bytes;
  }

  private <T> GenericRecord testJsonDecoder
    (String testType, byte[] bytes, T entityObj) throws IOException {

    ReflectData rdata = getReflectData();

    Schema schema = rdata.getSchema(entityObj.getClass());
    GenericDatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord>(schema);

    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes));
    GenericRecord r = datumReader.read(null, decoder);
    return r;
  }

  /**
   * Create a POJO having polymorphic fields
   */
  private Showroom buildShowroom () {
    Showroom sr = new Showroom ();

    Item mostSelling = new Chair();
    mostSelling.setPrice (20);
    Item leastSelling = new SwivelChair();
    leastSelling.setPrice (30);

    List<Item> trending = new ArrayList<Item>();
    trending.add (new Chair());
    trending.add (new Table());
    trending.add (new SwivelChair());

    sr.setMostSelling(mostSelling);
    sr.setLeastSelling(leastSelling);
    sr.setTrending(trending);

    return sr;
  }

  private void log (String msg) {
    System.out.println (msg);
  }

  private static class Item {
    String id = "500";
    int price = 10;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public int getPrice() {
      return price;
    }
    public void setPrice(int price) {
      this.price = price;
    }
  }

  private static class Chair extends Item {
    String color = "blue";

    public String getColor() {
      return color;
    }
    public void setColor(String color) {
      this.color = color;
    }
  }

  private static class Table extends Item {
    int height = 10;
    boolean hasDrawers = true;

    public int getHeight() {
      return height;
    }
    public void setHeight(int height) {
      this.height = height;
    }
    public boolean isHasDrawers() {
      return hasDrawers;
    }
    public void setHasDrawers(boolean hasDrawers) {
      this.hasDrawers = hasDrawers;
    }
  }

  private static class SwivelChair extends Chair {
    int percentDiscount = 10;

    public int getPercentDiscount() {
      return percentDiscount;
    }
    public void setPercentDiscount(int percentDiscount) {
      this.percentDiscount = percentDiscount;
    }
  }

  private static class Showroom {
    Item mostSelling;
    Item leastSelling;
    List<Item> trending;
    public Item getMostSelling() {
      return mostSelling;
    }
    public void setMostSelling(Item mostSelling) {
      this.mostSelling = mostSelling;
    }
    public Item getLeastSelling() {
      return leastSelling;
    }
    public void setLeastSelling(Item leastSelling) {
      this.leastSelling = leastSelling;
    }
    public List<Item> getTrending() {
      return trending;
    }
    public void setTrending(List<Item> trending) {
      this.trending = trending;
    }
  }
}
