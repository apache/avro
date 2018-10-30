/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Param;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectData.AllowNull;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;

public class TestParamTypes {

  private static class Pair<K, V> {

    private K key;
    private V value;

    public Pair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public Pair() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Pair<?, ?> pair = (Pair<?, ?>) o;
      return Objects.equals(getKey(), pair.getKey()) &&
        Objects.equals(getValue(), pair.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getKey(), getValue());
    }

    public K getKey() {
      return key;
    }

    public void setKey(K key) {
      this.key = key;
    }

    public V getValue() {
      return value;
    }

    public void setValue(V value) {
      this.value = value;
    }
  }

  private static class Pair2<K, V> {

    private int id;
    private K key;
    private V value;

    public Pair2(int id, K key, V value) {
      this.id = id;
      this.key = key;
      this.value = value;
    }

    private Pair2() {
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public K getKey() {
      return key;
    }

    public void setKey(K key) {
      this.key = key;
    }

    public V getValue() {
      return value;
    }

    public void setValue(V value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Pair2<?, ?> pair2 = (Pair2<?, ?>) o;
      return getId() == pair2.getId() &&
        Objects.equals(getKey(), pair2.getKey()) &&
        Objects.equals(getValue(), pair2.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getId(), getKey(), getValue());
    }
  }

  private static class TestPair2 {

    private Pair<Integer, String> value1;
    private Pair<Long, Double> value2;
    private Pair<Long, Double> value3;

    public TestPair2(Pair<Integer, String> value1,
      Pair<Long, Double> value2,
      Pair<Long, Double> value3) {
      this.value1 = value1;
      this.value2 = value2;
      this.value3 = value3;
    }

    public TestPair2() {
    }

    public Pair<Integer, String> getValue1() {
      return value1;
    }

    public void setValue1(Pair<Integer, String> value1) {
      this.value1 = value1;
    }

    public Pair<Long, Double> getValue2() {
      return value2;
    }

    public void setValue2(Pair<Long, Double> value2) {
      this.value2 = value2;
    }

    public Pair<Long, Double> getValue3() {
      return value3;
    }

    public void setValue3(Pair<Long, Double> value3) {
      this.value3 = value3;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestPair2 testPair2 = (TestPair2) o;
      return Objects.equals(getValue1(), testPair2.getValue1()) &&
        Objects.equals(getValue2(), testPair2.getValue2()) &&
        Objects.equals(getValue3(), testPair2.getValue3());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue1(), getValue2(), getValue3());
    }
  }

  private static class TestPair3 {

    private Integer id;
    private Pair2<Long, Double> value2;
    private Pair2<Long, Double> value3;

    public TestPair3(Integer id,
      Pair2<Long, Double> value2,
      Pair2<Long, Double> value3) {
      this.id = id;
      this.value2 = value2;
      this.value3 = value3;
    }

    public TestPair3() {
    }

    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    public Pair2<Long, Double> getValue2() {
      return value2;
    }

    public void setValue2(Pair2<Long, Double> value2) {
      this.value2 = value2;
    }

    public Pair2<Long, Double> getValue3() {
      return value3;
    }

    public void setValue3(Pair2<Long, Double> value3) {
      this.value3 = value3;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestPair3 testPair3 = (TestPair3) o;
      return Objects.equals(getId(), testPair3.getId()) &&
        Objects.equals(getValue2(), testPair3.getValue2()) &&
        Objects.equals(getValue3(), testPair3.getValue3());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getId(), getValue2(), getValue3());
    }
  }

  private static class TestMap1 {

    private Map<Integer, String> value1;

    public TestMap1() {
    }

    public TestMap1(Map<Integer, String> value1) {
      this.value1 = value1;
    }

    public Map<Integer, String> getValue1() {
      return value1;
    }

    public void setValue1(Map<Integer, String> value1) {
      this.value1 = value1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestMap1 testMap1 = (TestMap1) o;
      return Objects.equals(getValue1(), testMap1.getValue1());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue1());
    }
  }

  private static class TestMap2 {

    private Map<Integer, String> value1;
    private Map<Integer, String> value2;

    public TestMap2(Map<Integer, String> value1,
      Map<Integer, String> value2) {
      this.value1 = value1;
      this.value2 = value2;
    }

    public TestMap2() {
    }

    public Map<Integer, String> getValue1() {
      return value1;
    }

    public void setValue1(Map<Integer, String> value1) {
      this.value1 = value1;
    }

    public Map<Integer, String> getValue2() {
      return value2;
    }

    public void setValue2(Map<Integer, String> value2) {
      this.value2 = value2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestMap2 testMap2 = (TestMap2) o;
      return Objects.equals(getValue1(), testMap2.getValue1()) &&
        Objects.equals(getValue2(), testMap2.getValue2());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue1(), getValue2());
    }
  }

  @Test
  public void shouldParseRecordToParamSchema() throws IOException {
    Schema rootSchema = new Schema.Parser().parse(
      "[\"null\",{\"type\":\"param\",\"values\":{\"type\":\"record\",\"name\":\"PairMetricTypeLong\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}},{\"name\":\"value\",\"type\":\"long\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}}]");

    Schema rootParamSchema = new Schema.Parser().parse(
      "{\"type\":\"param\",\"values\":{\"type\":\"record\",\"name\":\"PairMetricTypeLong\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}},{\"name\":\"value\",\"type\":\"long\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}}");

    GenericData data = ReflectData.AllowNull.get();
    Schema schema = new Parser().parse(
      "{\"type\":\"record\",\"name\":\"PairMetricTypeLong\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}},{\"name\":\"value\",\"type\":\"long\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}");
    Record record = new Record(schema);

    Schema enumSchema = new Parser().parse(
      "{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}}");
    record.put("key", new EnumSymbol(enumSchema, "ABSOLUTE"));
    record.put("value", 135L);

    Param param = new Param(rootParamSchema, record);
    assertEquals(1, data.resolveUnion(rootSchema, param));

    AllowNull reflectData = new AllowNull();
    SerDe<Param> serDe = new SerDe<Param>(reflectData, SerDeType.json);
    byte[] bytes = serDe.serialize(param, rootSchema);
    assertNotNull(bytes);
  }

  @Test
  public void shouldTest_Pair() throws IOException {
    TestPair2 testPair2 = getTestPair2();

    ReflectData.AllowNull reflectData = new ReflectData.AllowNull();
    Schema schema = reflectData.getSchema(TestPair2.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    SerDe<TestPair2> serDe = new SerDe<TestPair2>(reflectData);
    byte[] bytes = serDe.serialize(testPair2, schema);
    assertNotNull(bytes);

    TestPair2 testPair2_DeSerialized = serDe.deserialize(bytes, schema);
    assertNotNull(testPair2_DeSerialized);

    Pair<Integer, String> value1 = testPair2_DeSerialized.getValue1();
    assertEquals(value1.getClass(), Pair.class);
  }

  @Test
  public void shouldTest_Pair_JsonSerDe() throws IOException {
    TestPair2 testPair2 = getTestPair2();

    ReflectData.AllowNull reflectData = new ReflectData.AllowNull();
    Schema schema = reflectData.getSchema(TestPair2.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    SerDe<TestPair2> jsonSerDe = new SerDe<TestPair2>(reflectData, SerDeType.json);
    byte[] jsonBytes = jsonSerDe.serialize(testPair2, schema);
    assertNotNull(jsonBytes);
    System.out.println(new String(jsonBytes));

    TestPair2 testPair2_DeSerialized = jsonSerDe.deserialize(jsonBytes, schema);
    assertNotNull(testPair2_DeSerialized);
  }

  private TestPair2 getTestPair2() {
    Pair<Integer, String> pair1 = new Pair<Integer, String>();
    pair1.setKey(102);
    pair1.setValue("one_not_two");

    Pair<Long, Double> pair2 = new Pair<Long, Double>();
    pair2.setKey(1001L);
    pair2.setValue(1001.00);

    Pair<Long, Double> pair3 = new Pair<Long, Double>();
    pair3.setKey(2001L);
    pair3.setValue(2001.00);

    return new TestPair2(pair1, pair2, pair3);
  }


  private enum SerDeType {
    json, avro
  }

  private class SerDe<T> {

    private final ReflectData reflectData;
    private final SerDeType serDeType;

    SerDe(ReflectData reflectData) {
      this.reflectData = reflectData;
      this.serDeType = SerDeType.avro;
    }

    SerDe(ReflectData reflectData, SerDeType serDeType) {
      this.reflectData = reflectData;
      this.serDeType = serDeType;
    }

    byte[] serialize(T object, Schema schema) throws IOException {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      Encoder encoder;
      if (SerDeType.json.equals(serDeType)) {
        encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
      } else {
        encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      }
      DatumWriter<T> datumWriter = new ReflectDatumWriter<T>(schema, reflectData);
      datumWriter.write(object, encoder);

      encoder.flush();
      outputStream.close();
      return outputStream.toByteArray();
    }

    T deserialize(byte[] bytes, Schema schema) throws IOException {
      DatumReader<T> datumReader = new ReflectDatumReader<T>(schema, schema,
        reflectData);
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
      Decoder decoder;
      if (SerDeType.json.equals(serDeType)) {
        decoder = DecoderFactory.get().jsonDecoder(schema, inputStream);
      } else {
        decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      }

      return datumReader.read(null, decoder);
    }
  }

  @Test
  public void shouldTest_Pair_WithGenericAndNormalTypes() throws IOException {
    Pair2<Long, Double> pair2_1 = new Pair2<Long, Double>();
    pair2_1.setId(5001);
    pair2_1.setKey(1001L);
    pair2_1.setValue(1001.00);

    Pair2<Long, Double> pair2_2 = new Pair2<Long, Double>();
    pair2_2.setId(5002);
    pair2_2.setKey(2001L);
    pair2_2.setValue(2001.00);
    TestPair3 testPair3 = new TestPair3(145, pair2_1, pair2_2);

    ReflectData reflectData = new ReflectData.AllowNull();

    Schema schema = reflectData.getSchema(TestPair3.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    SerDe<TestPair3> serDe = new SerDe<TestPair3>(reflectData);
    byte[] bytes = serDe.serialize(testPair3, schema);
    assertNotNull(bytes);

    TestPair3 testPair3_DeSerialized = serDe.deserialize(bytes, schema);
    assertNotNull(testPair3_DeSerialized);

    assertEquals(testPair3.getId(), testPair3_DeSerialized.getId());
    Pair2<Long, Double> value2 = testPair3_DeSerialized.getValue2();
    assertEquals(value2.getClass(), Pair2.class);
  }

  @Test
  public void shouldGenerateAndParseSchema() {
    ReflectData reflectData = new ReflectData.AllowNull();
    Schema schema = reflectData.getSchema(TestPair2.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    Schema parsedSchema = new Parser().parse(schemaJson);
    assertNotNull(parsedSchema);
    assertEquals(schema, parsedSchema);
  }

  @Test
  public void shouldTest_Map1() throws IOException {
    //Map<Integer, String> map1 = new ConcurrentHashMap<Integer, String>();
    Map<Integer, String> map1 = new HashMap<Integer, String>();
    map1.put(1, "one");
    map1.put(2, "two");

    TestMap1 testMap1 = new TestMap1(map1);

    ReflectData reflectData = new ReflectData.AllowNull();
    Schema schema = reflectData.getSchema(TestMap1.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    SerDe<TestMap1> serDe = new SerDe<TestMap1>(reflectData);
    byte[] bytes = serDe.serialize(testMap1, schema);
    assertNotNull(bytes);

    TestMap1 testMap_deSerialized = serDe.deserialize(bytes, schema);
    assertNotNull(testMap_deSerialized);
  }

  @Test
  public void shouldTest_Map2() throws IOException {
    HashMap<Integer, String> map1 = new HashMap<Integer, String>();
    map1.put(1, "one");
    map1.put(2, "two");

    Map<Integer, String> map2 = new HashMap<Integer, String>();
    map1.put(11, "eleven");
    map1.put(12, "twelve");

    TestMap2 testMap2 = new TestMap2(map1, map2);

    ReflectData reflectData = new ReflectData.AllowNull();
    Schema schema = reflectData.getSchema(TestMap2.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    SerDe<TestMap2> serDe = new SerDe<TestMap2>(reflectData);
    byte[] bytes = serDe.serialize(testMap2, schema);
    assertNotNull(bytes);

    TestMap2 testMap_deSerialized = serDe.deserialize(bytes, schema);
    assertNotNull(testMap_deSerialized);
  }

  private static class TestSet {

    private Set<String> data1;

    public TestSet(Set<String> data1) {
      this.data1 = data1;
    }

    public TestSet() {
    }

    public Set<String> getData1() {
      return data1;
    }

    public void setData1(Set<String> data1) {
      this.data1 = data1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestSet testSet = (TestSet) o;
      return Objects.equals(getData1(), testSet.getData1());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getData1());
    }
  }

  @Test
  public void shouldSerDe_JavaSets() throws IOException {
    Set<String> setData = new HashSet<String>();
    setData.add("one");
    setData.add("two");
    TestSet testSet = new TestSet(setData);

    AllowNull reflectData = new AllowNull();
    Schema schema = reflectData.getSchema(TestSet.class);

    SerDe<TestSet> serDe = new SerDe<TestSet>(reflectData);
    byte[] bytes = serDe.serialize(testSet, schema);
    assertNotNull(bytes);

    TestSet testSet_DeSerialized = serDe.deserialize(bytes, schema);
    assertNotNull(testSet_DeSerialized);
  }
}
