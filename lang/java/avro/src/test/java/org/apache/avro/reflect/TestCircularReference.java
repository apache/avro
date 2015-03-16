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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericArray;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test circular reference support
 */
@RunWith(Parameterized.class)
public class TestCircularReference {

  private ReflectData rdata = null;
  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { new ReflectData.AllowNull() },
        { new ReflectData() },
    });
  }

  public TestCircularReference(ReflectData rdata) {
    this.rdata = rdata;
    this.rdata.setResolvingCircularRefs(true);
  }

  @Test
  public void testSimplestCircularReference() throws Exception {

    SimpleParent entityObj1 = buildSimpleParent();
    SimpleParent entityObj2 = buildSimpleParent();

    List<GenericRecord> records = testCircularSerialization(
        "SimplestCircularReferenceTest", entityObj1, entityObj2);
    assertEquals ("Unable to read all records", 2, records.size());
    SimpleParent record = (SimpleParent)records.get(0);
    SimpleChild child = record.getChild();
    assertNotNull ("Unable to read child object", child);
    Object parent = child.getParent();
    assertEquals ("Unable to restore circular reference", record, parent);
    
    // Test JSON serialization and deserialization
    byte[] jsonBytes = testJsonEncoder (entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    SimpleParent jsonRecord = testJsonDecoder(jsonBytes, true, entityObj1);
    assertEquals ("JSON decoder unable to restore circular reference",
        entityObj1.child.parent.parentName, jsonRecord.child.parent.parentName);
    
    jsonRecord = testJsonDecoder(jsonBytes, false, entityObj1);
    assertTrue ("JSON decoder should work without restoring circular references",
        ((Object)jsonRecord.child.parent) instanceof CircularRef);
  }

  @Test
  public void testListCircularReference() throws Exception {

    School entityObj1 = buildSchool();
    School entityObj2 = buildSchool();

    List<GenericRecord> records = testCircularSerialization(
        "ListCircularReferenceTest", entityObj1, entityObj2);
    assertEquals ("Unable to read all records", 2, records.size());
    School record = (School)records.get(0);
    testSchoolObjects (record);
    
    // Test JSON serialization and deserialization
    byte[] jsonBytes = testJsonEncoder (entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    Object jsonRecord = testJsonDecoder(jsonBytes, true, entityObj1);
    assertNotNull ("JSON decoder should work when restoring circular references", jsonRecord);
    jsonRecord = testJsonDecoder(jsonBytes, false, entityObj1);
    assertNotNull ("JSON decoder should work when not restoring circular references", jsonRecord);
  }

  @Test
  public void testMapCircularReference() throws Exception {

    SchoolSearchAgent entityObj1 = buildSchoolSearchAgent();
    SchoolSearchAgent entityObj2 = buildSchoolSearchAgent();

    List<GenericRecord> records = testCircularSerialization(
        "MapCircularReferenceTest", entityObj1, entityObj2);
    assertEquals ("Unable to read all records", 2, records.size());
    SchoolSearchAgent record = (SchoolSearchAgent)records.get(0);
    Object schoolsPerZipcode = record.getSchoolsPerZipcode();
    assertNotNull ("Unable to read HashMap <String, List<School>>", schoolsPerZipcode);
    assertTrue (schoolsPerZipcode instanceof HashMap);
    HashMap <String, List<School>> m = (HashMap <String, List<School>>)schoolsPerZipcode;
    assertTrue (m.size() > 0);
    ArrayList schools = (ArrayList)m.values().iterator().next();
    assertTrue (schools.size() > 0);
    School school = (School)schools.get(0);
    testSchoolObjects (school);
    Object agent = school.getAgent();
    assertEquals ("Unable to restore circular reference", record, agent);

    // Test JSON serialization and deserialization
    byte[] jsonBytes = testJsonEncoder (entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    Object jsonRecord = testJsonDecoder(jsonBytes, true, entityObj1);
    assertNotNull ("JSON decoder should work when restoring circular references", jsonRecord);
    jsonRecord = testJsonDecoder(jsonBytes, false, entityObj1);
    assertNotNull ("JSON decoder should work when not restoring circular references", jsonRecord);
  }

  private void testSchoolObjects (School s) {
    List<Child> children = s.getChildren();
    assertNotNull ("Unable to read List<Child> object", children);
    assertTrue (children.size() > 0);
    Child child = children.get(0);
    Parent parent = child.getParent();
    School school = child.getSchool();
    assertEquals ("Unable to restore circular reference", s, school);
    Object childInParent = parent.getChild();
    assertEquals ("Unable to restore circular reference", child, childInParent);

    County county = s.getCounty();
    assertNotNull ("Unable to read county", county);
    HashMap<School, List<Child>> students = county.getStudents();
    assertNotNull ("Unable to read students", students);

    School s2 = students.entrySet().iterator().next().getKey();
    List<Child> childStudents = students.entrySet().iterator().next().getValue();
    assertEquals ("Unable to restore circular reference", s, s2);
    assertEquals ("Unable to restore circular reference", s.getChildren(), childStudents);
    log ("Read " + childStudents.size() + " students for school " + s2 + " in the county");
  }

  @Test
  public void testCircularLinkedList() throws Exception {

    CircularList entityObj1 = buildCircularList();

    List<GenericRecord> records = testCircularSerialization(
        "CircularLinkedListTest", entityObj1);
    assertEquals ("Unable to read all records", 1, records.size());
    CircularList l1 = (CircularList)records.get(0);
    Object nodeData = l1.getNodeData();
    assertEquals ("Unable to read first-node", "ABC", nodeData.toString());

    CircularList l2 = l1.getNext();
    nodeData = l2.getNodeData();
    assertEquals ("Unable to read second-node", "DEF", nodeData.toString());

    CircularList l3 = l2.getNext();
    nodeData = l3.getNodeData();
    assertEquals ("Unable to read second-node", "GHI", nodeData.toString());

    // Test JSON serialization and deserialization
    byte[] jsonBytes = testJsonEncoder (entityObj1);
    assertNotNull ("Unable to serialize using jsonEncoder", jsonBytes);
    Object jsonRecord = testJsonDecoder(jsonBytes, true, entityObj1);
    assertNotNull ("JSON decoder should work when restoring circular references", jsonRecord);
    jsonRecord = testJsonDecoder(jsonBytes, false, entityObj1);
    assertNotNull ("JSON decoder should work when not restoring circular references", jsonRecord);
  }

  /**
   * Test the serialization of records having circular references.
   * To enable circular references, only option is required to be set by clients:<BR>
   * <i>    ReflectData.setResolvingCircularRefs(true);   </i><BR>
   * The same reflectData should be used when constructing ReflectDatumWriter.<BR>
   * <BR>
   * By enabling this option:<BR>
   * 1) Schema for every record-type-field is converted into a UNION of circular_ref
   * and record. (ReflectData.AllowNull is supported).<BR>
   * 3) When circular reference is hit, the ID for that record is written instead
   * of the record itself, preventing infinite recursion.<BR>
   */
  public <T> List<GenericRecord> testCircularSerialization
      (String testType, T ... entityObjs) throws Exception {

    log ("---- Beginning " + testType + " ----");
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
    assertTrue ("Unable to serialize circular references in " + testType,
        bytes.length > 0);

    testNormalDeserialization(testType, bytes, entityObjs);
    List<GenericRecord> records =
        testCircularDeserialization(testType, bytes, entityObjs);

    return records;
  }

  /**
   * Test that it is possible to deserialize an Avro output which originally had
   * circular references. Nothing special is required to deserialize since
   * circular serialization created a UNION schema for every record
   * such that every record could either be a circular_ref or a record.
   * Then circular serialization stored "string" ID instead of actual records
   * whenever it found a record already written before. So the final
   * serializer-output is readable by any deserializer (even by those who do
   * not support circular references). This kind of support is especially useful
   * for Avro deserializers embedded in third-party libraries (like Hive/Pig).
   */
  private <T> void testNormalDeserialization
       (String testType, byte[] bytes, T ... entityObjs) throws IOException {
    String avroString = new String (bytes);
    log ("Avro serialized string:\n" + avroString);

    ReflectDatumReader<Object> datumReader =
        new ReflectDatumReader<Object> ();
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<Object> fileReader =
        new DataFileReader<Object>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    assertNotNull("Unable to get schema for circular reference in " + testType, schema);
    log ("--- Read schema successfully ----");
    Object record = null;
    int i=0;
    while (fileReader.hasNext()) {
      i++;
      record = fileReader.next(record);
      assertNotNull("Unable to read records for circular reference in " + testType, record);
      log ("Avro record (without cycles)" + i + ") " + record.toString());
    }
    assertEquals ("Unable to read same number of records as serialized",
        entityObjs.length, i);
  }

  /**
   * This function tests that it is possible to generate the original circular reference
   * by using <i>ReflectData.setResolvingCircularRefs (true)</i><BR>
   * This API is useful when multiple threads are deserializing while sharing
   * the same {@link ReflectData}. Each ReflectDatumReader starts as a
   * non-circular-reference reader if this API is not used.
   * Restoring circular references could be useful when the clients want to
   * reconstruct the actual cycles in their language specific reconstructed
   * object-models.
   * @see #testNormalDeserialization(String, byte[], Object...)
   */
  private <T> List<GenericRecord> testCircularDeserialization
      (String testType, byte[] bytes, T ... entityObjs) throws IOException {

    ReflectData data = new ReflectData();
    data.setResolvingCircularRefs(true);
    ReflectDatumReader<GenericRecord> datumReader =
        new ReflectDatumReader<GenericRecord> (data);
    SeekableByteArrayInput avroInputStream = new SeekableByteArrayInput(bytes);
    DataFileReader<GenericRecord> fileReader =
        new DataFileReader<GenericRecord>(avroInputStream, datumReader);

    Schema schema = fileReader.getSchema();
    assertNotNull("Unable to get schema for circular reference in " + testType, schema);
    GenericRecord record = null;
    List<GenericRecord> records = new ArrayList<GenericRecord> ();
    while (fileReader.hasNext()) {
      records.add (fileReader.next(record));
    }
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
    log ("JSON encoder output:\n" + new String(bytes));
    return bytes;
  }
  
  private <T> T testJsonDecoder(byte[] bytes, boolean resolveRefs, T entityObj)
      throws IOException {

    Schema schema = rdata.getSchema(entityObj.getClass());
    ReflectData data = new ReflectData();
    data.setResolvingCircularRefs(resolveRefs);
    ReflectDatumReader<T> datumReader =
      new ReflectDatumReader<T>(schema, schema, data);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(bytes));
    T r = datumReader.read(null, decoder);
    log (r.toString());
    return r;
  }

  private SimpleParent buildSimpleParent () {
    SimpleParent p = new SimpleParent ();
    SimpleChild c = new SimpleChild ();
    p.setChild (c);
    c.setParent(p);
    return p;
  }
  
  private Parent buildParent () {
    Parent p = new Parent ();
    Child c = new Child (p);
    p.setChild (c);
    return p;
  }

  private School buildSchool () {
    School school = new School ();
    school.setChildren(new ArrayList<Child>());
    for (int i=0; i<2; i++) {
      Parent p = buildParent();
      school.getChildren().add(p.getChild());
      p.getChild().setSchool(school);
    }
    // Put some dummy value for 'agent' because AllowNull is not being used.
    // So it will not handle 'null'
    if (!(rdata instanceof ReflectData.AllowNull)) {
      SchoolSearchAgent agent = new SchoolSearchAgent ();
      agent.setAgentId(1);
      agent.setAgentName("Foo");
      agent.setSchoolsPerZipcode(new HashMap<String, List<School>>());
      school.setAgent(agent);
    } else {
      // For AllowNull, dummy-values are not required since we want to test
      // actual null values also.
      // In fact, we make some circular references as null to make sure
      // nulls are supported there also
      school.children.get(1).setSchool(null);
      school.children.get(1).setParent(null);
    }

    County county = new County();
    school.setCounty(county);
    county.setStudents(new HashMap<School, List<Child>>());
    county.getStudents().put(school, school.getChildren());
    return school;
  }

  private SchoolSearchAgent buildSchoolSearchAgent () {
    SchoolSearchAgent agent = new SchoolSearchAgent ();
    HashMap<String, List<School>> schoolsPerZipcode = 
        new HashMap <String, List<School>> ();
    agent.setSchoolsPerZipcode(schoolsPerZipcode);

    List<School> list1 = new ArrayList<School> ();
    schoolsPerZipcode.put("94086", list1);
    List<School> list2 = new ArrayList<School> ();
    schoolsPerZipcode.put("95050", list2);

    School school1 = buildSchool();
    School school2 = buildSchool();
    list1.add(school1);
    list1.add(school2);
    school1.setAgent(agent);
    school2.setAgent(agent);
    School school3 = buildSchool();
    list2.add(school3);
    school3.setAgent(agent);
    return agent;
  }

  private CircularList buildCircularList () {
    CircularList l1 = new CircularList();
    l1.setNodeData("ABC");

    CircularList l2 = new CircularList();
    l2.setNodeData("DEF");
    l1.setNext (l2);

    CircularList l3 = new CircularList();
    l3.setNodeData("GHI");
    l2.setNext (l3);

    l3.setNext (l1);
    return l1;
  }

  private void log (String msg) {
    System.out.println (msg);
  }

  private static class SimpleParent {
  
    String parentName = "John Sr";
    SimpleChild child;
  
    public String getParentName() {
      return parentName;
    }
    public void setParentName(String parentName) {
      this.parentName = parentName;
    }
    public SimpleChild getChild() {
      return child;
    }
    public void setChild(SimpleChild child) {
      this.child = child;
    }
    @Override
    public String toString() {
      return "SimpleParent [parentName=" + parentName + 
          ", child=" + (child==null?"null":child.childName) + "]";
    }
  };

  private static class SimpleChild {
  
    String childName = "John Jr";
    SimpleParent parent;
  
    public String getChildName() {
      return childName;
    }
    public void setChildName(String childName) {
      this.childName = childName;
    }
    public SimpleParent getParent() {
      return parent;
    }
    public void setParent(SimpleParent parent) {
      this.parent = parent;
    }
    @Override
    public String toString() {
      return "SimpleChild [childName=" + childName +
          ", parent=" + (parent==null?"null":parent.parentName) + "]";
    }
  };

  private static class Parent {

    String name = "John Sr";
    Child child;

    public Child getChild() {
      return child;
    }
    public void setChild(Child child) {
      this.child = child;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    @Override
    public String toString() {
      return "Parent [name=" + name + ", child=" + (child==null?"null":child.name) + "]";
    }
  };

  private static class Child {

    String name = "John Jr";
    Parent parent;
    School school;

    public Child() {}
    public Child(Parent parent) {
      this.parent = parent;
    }
    public Parent getParent() {
      return parent;
    }
    public void setParent(Parent parent) {
      this.parent = parent;
    }
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public School getSchool() {
      return school;
    }
    public void setSchool(School school) {
      this.school = school;
    }
    @Override
    public String toString() {
      return "Child [name=" + name + 
          ", parent=" + (parent==null?"null":parent.name) +
          ", school=" + (school==null?"null":school.name) +
          "]";
    }
  };

  private static class School {
    String name = "Hogwarts School";
    List<Child> children;
    Integer zipCode = 94086;
    SchoolSearchAgent agent;
    County county;

    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public List<Child> getChildren() {
      return children;
    }
    public void setChildren(List<Child> children) {
      this.children = children;
    }
    public Integer getZipCode() {
      return zipCode;
    }
    public void setZipCode(Integer zipCode) {
      this.zipCode = zipCode;
    }
    public SchoolSearchAgent getAgent() {
      return agent;
    }
    public void setAgent(SchoolSearchAgent agent) {
      this.agent = agent;
    }
    public County getCounty() {
      return county;
    }
    public void setCounty(County county) {
      this.county = county;
    }
    @Override
    public String toString() {
      return "School [name=" + name +
          ", children=" + (children==null?"null":children.size()) +
          ", zipCode=" + zipCode +
          ", agent=" + (agent==null?"null":agent.agentName) + "]";
    }
  }

  private static class SchoolSearchAgent {
    Integer agentId = 101;
    String agentName = "Agent Foo";
    HashMap <String, List<School>> schoolsPerZipcode;

    public Integer getAgentId() {
      return agentId;
    }
    public void setAgentId(Integer agentId) {
      this.agentId = agentId;
    }
    public String getAgentName() {
      return agentName;
    }
    public void setAgentName(String agentName) {
      this.agentName = agentName;
    }
    public HashMap<String, List<School>> getSchoolsPerZipcode() {
      return schoolsPerZipcode;
    }
    public void setSchoolsPerZipcode(HashMap<String, List<School>> schoolsPerZipcode) {
      this.schoolsPerZipcode = schoolsPerZipcode;
    }
    @Override
    public String toString() {
      return "SchoolSearchAgent [agentId=" + agentId + ", agentName=" + agentName
          + ", schoolsPerZipcode=" + (schoolsPerZipcode==null?"null":schoolsPerZipcode.size()) + "]";
    }
  };

  private static class County { // Tests non-string map-keys with circular references
    HashMap <School, List<Child>> students;
    public HashMap<School, List<Child>> getStudents() {
      return students;
    }
    public void setStudents(HashMap<School, List<Child>> students) {
      this.students = students;
    }
  }

  private static class CircularList {
    String nodeData;
    CircularList next;

    public String getNodeData () {
      return nodeData;
    }
    public void setNodeData (String nodeData) {
      this.nodeData = nodeData;
    }
    public CircularList getNext() {
      return next;
    }
    public void setNext (CircularList next) {
      this.next = next;
    }
    @Override
    public String toString() {
      return "CircularList [nodeData=" + nodeData +
          ", next=" + (next==null?"null":next.nodeData) + "]";
    }
  }
}
