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
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * Test serialization and de-serialization of non-string map-keys
 */
public class TestNonStringMapKeys {

  @Test
  public void testNonStringMapKeys() throws Exception {

    Company entityObj1 = buildCompany();
    Company entityObj2 = buildCompany();

    String testType = "NonStringKeysTest";
    Company [] entityObjs = {entityObj1, entityObj2};
    byte[] bytes = testSerialization(testType, entityObj1, entityObj2);
    List<GenericRecord> records = 
      (List<GenericRecord>) testGenericDatumRead(testType, bytes, entityObjs);

    GenericRecord record = records.get(0);
    Object employees = record.get("employees");
    assertTrue ("Unable to read 'employees' map", employees instanceof GenericArray);
    GenericArray arrayEmployees = ((GenericArray)employees);
    Object employeeRecord = arrayEmployees.get(0);
    assertTrue (employeeRecord instanceof GenericRecord);
    Object key = ((GenericRecord)employeeRecord).get(ReflectData.NS_MAP_KEY);
    Object value = ((GenericRecord)employeeRecord).get(ReflectData.NS_MAP_VALUE);
    assertTrue (key instanceof GenericRecord);
    assertTrue (value instanceof GenericRecord);
    //Map stored: 1:foo, 2:bar
    Object id = ((GenericRecord)key).get("id");
    Object name = ((GenericRecord)value).get("name").toString();
    assertTrue (
      (id.equals(1) && name.equals("Foo")) || 
      (id.equals(2) && name.equals("Bar"))
    );

    List<Company> records2 =
      (List<Company>) testReflectDatumRead(testType, bytes, entityObjs);
    Company co = records2.get(0);
    log ("Read: " + co);
    assertNotNull (co.getEmployees());
    assertEquals (2, co.getEmployees().size());
    Iterator<Entry<EmployeeId, EmployeeInfo>> itr = co.getEmployees().entrySet().iterator();
    while (itr.hasNext()) {
      Entry<EmployeeId, EmployeeInfo> e = itr.next();
      id = e.getKey().getId();
      name = e.getValue().getName();
      assertTrue (
        (id.equals(1) && name.equals("Foo")) || 
        (id.equals(2) && name.equals("Bar"))
      );
    }


    byte[] jsonBytes = testJsonEncoder (testType, entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = testJsonDecoder(testType, jsonBytes, entityObj1);
    assertEquals ("JSON decoder output not same as Binary Decoder", record, jsonRecord);
  }
  
  @Test
  public void testNonStringMapKeysInNestedMaps() throws Exception {

    Company2 entityObj1 = buildCompany2();

    String testType = "NestedMapsTest";
    Company2 [] entityObjs = {entityObj1};
    byte[] bytes = testSerialization(testType, entityObj1);
    List<GenericRecord> records =
      (List<GenericRecord>) testGenericDatumRead(testType, bytes, entityObjs);

    GenericRecord record = records.get(0);
    Object employees = record.get("employees");
    assertTrue ("Unable to read 'employees' map", employees instanceof GenericArray);
    GenericArray employeesMapArray = ((GenericArray)employees);
    
    Object employeeMapElement = employeesMapArray.get(0);
    assertTrue (employeeMapElement instanceof GenericRecord);
    Object key = ((GenericRecord)employeeMapElement).get(ReflectData.NS_MAP_KEY);
    Object value = ((GenericRecord)employeeMapElement).get(ReflectData.NS_MAP_VALUE);
    assertEquals (11, key);
    assertTrue (value instanceof GenericRecord);
    GenericRecord employeeInfo = (GenericRecord)value;
    Object name = employeeInfo.get("name").toString();
    assertEquals ("Foo", name);
    
    Object companyMap = employeeInfo.get("companyMap");
    assertTrue (companyMap instanceof GenericArray);
    GenericArray companyMapArray = (GenericArray)companyMap;
    
    Object companyMapElement = companyMapArray.get(0);
    assertTrue (companyMapElement instanceof GenericRecord);
    key = ((GenericRecord)companyMapElement).get(ReflectData.NS_MAP_KEY);
    value = ((GenericRecord)companyMapElement).get(ReflectData.NS_MAP_VALUE);
    assertEquals (14, key);
    if (value instanceof Utf8)
      value = ((Utf8)value).toString();
    assertEquals ("CompanyFoo", value);
    
    List<Company2> records2 =
      (List<Company2>) testReflectDatumRead(testType, bytes, entityObjs);
    Company2 co = records2.get(0);
    log ("Read: " + co);
    assertNotNull (co.getEmployees());
    assertEquals (1, co.getEmployees().size());
    Iterator<Entry<Integer, EmployeeInfo2>> itr = co.getEmployees().entrySet().iterator();
    while (itr.hasNext()) {
      Entry<Integer, EmployeeInfo2> e = itr.next();
      Integer id = e.getKey();
      name = e.getValue().getName();
      assertTrue (id.equals(11) && name.equals("Foo"));
      assertEquals ("CompanyFoo", e.getValue().companyMap.values().iterator().next());
    }


    byte[] jsonBytes = testJsonEncoder (testType, entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = testJsonDecoder(testType, jsonBytes, entityObj1);
    assertEquals ("JSON decoder output not same as Binary Decoder", record, jsonRecord);
  }

  @Test
  public void testRecordNameInvariance() throws Exception {

    SameMapSignature entityObj1 = buildSameMapSignature();

    String testType = "RecordNameInvariance";
    SameMapSignature [] entityObjs = {entityObj1};
    byte[] bytes = testSerialization(testType, entityObj1);
    List<GenericRecord> records =
      (List<GenericRecord>) testGenericDatumRead(testType, bytes, entityObjs);

    GenericRecord record = records.get(0);
    Object map1obj = record.get("map1");
    assertTrue ("Unable to read map1", map1obj instanceof GenericArray);
    GenericArray map1array = ((GenericArray)map1obj);
    
    Object map1element = map1array.get(0);
    assertTrue (map1element instanceof GenericRecord);
    Object key = ((GenericRecord)map1element).get(ReflectData.NS_MAP_KEY);
    Object value = ((GenericRecord)map1element).get(ReflectData.NS_MAP_VALUE);
    assertEquals (1, key);
    assertEquals ("Foo", value.toString());

    Object map2obj = record.get("map2");
    assertEquals (map1obj, map2obj);
    
    List<SameMapSignature> records2 =
      (List<SameMapSignature>) testReflectDatumRead(testType, bytes, entityObjs);
    SameMapSignature entity = records2.get(0);
    log ("Read: " + entity);
    assertNotNull (entity.getMap1());
    assertEquals (1, entity.getMap1().size());
    Iterator<Entry<Integer, String>> itr = entity.getMap1().entrySet().iterator();
    while (itr.hasNext()) {
      Entry<Integer, String> e = itr.next();
      key = e.getKey();
      value = e.getValue();
      assertEquals (1, key);
      assertEquals ("Foo", value.toString());
    }
    assertEquals (entity.getMap1(), entity.getMap2());


    ReflectData rdata = ReflectData.get();
    Schema schema = rdata.getSchema(SameMapSignature.class);
    Schema map1schema = schema.getField("map1").schema().getElementType();
    Schema map2schema = schema.getField("map2").schema().getElementType();
    log ("Schema for map1 = " + map1schema);
    log ("Schema for map2 = " + map2schema);
    assertEquals (map1schema.getFullName(), "org.apache.avro.reflect.PairIntegerString");
    assertEquals (map1schema, map2schema);


    byte[] jsonBytes = testJsonEncoder (testType, entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    GenericRecord jsonRecord = testJsonDecoder(testType, jsonBytes, entityObj1);
    assertEquals ("JSON decoder output not same as Binary Decoder", 
      record.get("map1"), jsonRecord.get("map1"));
    assertEquals ("JSON decoder output not same as Binary Decoder", 
      record.get("map2"), jsonRecord.get("map2"));
  }

  /**
   * Test serialization of non-string map-key POJOs
   */
  public <T> byte[] testSerialization(String testType, T ... entityObjs) throws Exception {

    log ("---- Beginning " + testType + " ----");
    T entityObj1 = entityObjs[0];
    ReflectData rdata = ReflectData.AllowNull.get();

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

    ReflectData rdata = ReflectData.AllowNull.get();

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

    ReflectData rdata = ReflectData.AllowNull.get();

    Schema schema = rdata.getSchema(entityObj.getClass());
    GenericDatumReader<GenericRecord> datumReader =
      new GenericDatumReader<GenericRecord>(schema);

    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes));
    GenericRecord r = datumReader.read(null, decoder);
    return r;
  }

  /**
   * Create a POJO having non-string map-keys
   */
  private Company buildCompany () {
    Company co = new Company ();
    HashMap<EmployeeId, EmployeeInfo> employees = new HashMap<EmployeeId, EmployeeInfo>();
    co.setEmployees(employees);
    employees.put(new EmployeeId(1), new EmployeeInfo("Foo"));
    employees.put(new EmployeeId(2), new EmployeeInfo("Bar"));
    return co;
  }

  /**
   * Create a POJO having non-string map-keys
   * The objects inside that map should also have non-string map-keys
   */
  private Company2 buildCompany2 () {
    Company2 co = new Company2 ();
    HashMap<Integer, EmployeeInfo2> employees = new HashMap<Integer, EmployeeInfo2>();
    co.setEmployees(employees);
    
    EmployeeId2 empId = new EmployeeId2(1);
    EmployeeInfo2 empInfo = new EmployeeInfo2("Foo");
    HashMap<Integer, String> companyMap = new HashMap<Integer, String>();
    empInfo.setCompanyMap(companyMap);
    companyMap.put(14, "CompanyFoo");
    
    employees.put(11, empInfo);
    
    return co;
  }

  private SameMapSignature buildSameMapSignature () {
    SameMapSignature obj = new SameMapSignature();
    obj.setMap1(new HashMap<Integer, String>());
    obj.getMap1().put(1, "Foo");
    obj.setMap2(new HashMap<Integer, String>());
    obj.getMap2().put(1, "Foo");
    return obj;
  }

  private void log (String msg) {
    System.out.println (msg);
  }
}

class Company {
  HashMap <EmployeeId, EmployeeInfo> employees;

  public HashMap<EmployeeId, EmployeeInfo> getEmployees() {
    return employees;
  }
  public void setEmployees(HashMap<EmployeeId, EmployeeInfo> employees) {
    this.employees = employees;
  }
  @Override
  public String toString() {
    return "Company [employees=" + employees + "]";
  }
}

class EmployeeId {
  Integer id;

  public EmployeeId() {}
  public EmployeeId(Integer id) {
    this.id = id;
  }
  public Integer getId() {
    return id;
  }
  public void setId(Integer zip) {
    this.id = zip;
  }
  @Override
  public String toString() {
    return "EmployeeId [id=" + id + "]";
  }
}

class EmployeeInfo {
  String name;

  public EmployeeInfo() {}
  public EmployeeInfo(String name) {
    this.name = name;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  @Override
  public String toString() {
    return "EmployeeInfo [name=" + name + "]";
  }
}

class Company2 {
  HashMap <Integer, EmployeeInfo2> employees;

  public Company2() {}
  public HashMap<Integer, EmployeeInfo2> getEmployees() {
    return employees;
  }
  public void setEmployees(HashMap<Integer, EmployeeInfo2> employees) {
    this.employees = employees;
  }
  @Override
  public String toString() {
    return "Company2 [employees=" + employees + "]";
  }
}

class EmployeeId2 {
  Integer id;

  public EmployeeId2() {}
  public EmployeeId2(Integer id) {
    this.id = id;
  }
  public Integer getId() {
    return id;
  }
  public void setId(Integer zip) {
    this.id = zip;
  }
  @Override
  public String toString() {
    return "EmployeeId2 [id=" + id + "]";
  }
}

class EmployeeInfo2 {
  String name;
  HashMap<Integer, String> companyMap;

  public EmployeeInfo2() {}
  public EmployeeInfo2(String name) {
    this.name = name;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public HashMap<Integer, String> getCompanyMap() {
    return companyMap;
  }
  public void setCompanyMap(HashMap<Integer, String> companyMap) {
    this.companyMap = companyMap;
  }
  @Override
  public String toString() {
    return "EmployeeInfo2 [name=" + name + "]";
  }
}

class SameMapSignature {

  HashMap<Integer, String> map1;
  HashMap<Integer, String> map2;

  public HashMap<Integer, String> getMap1() {
    return map1;
  }
  public void setMap1(HashMap<Integer, String> map1) {
    this.map1 = map1;
  }
  public HashMap<Integer, String> getMap2() {
    return map2;
  }
  public void setMap2(HashMap<Integer, String> map2) {
    this.map2 = map2;
  }
}
