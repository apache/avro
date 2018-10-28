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
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
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
  public void shouldParseRecordToParamSchema() {
//    Schema rootSchema = new Schema.Parser().parse(
//      "[\"null\",{\"type\":\"param\",\"values\":{\"type\":\"record\",\"name\":\"PairMetricTypeLong\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}},{\"name\":\"value\",\"type\":\"long\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}}]");

    Schema rootSchema = new Schema.Parser().parse(
      "[\"null\",{\"type\":\"record\",\"name\":\"PairMetricTypeLong\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}},{\"name\":\"value\",\"type\":\"long\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}]");
    GenericData data = ReflectData.AllowNull.get();

    Schema schema = new Parser().parse(
      "{\"type\":\"record\",\"name\":\"PairMetricTypeLong\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}},{\"name\":\"value\",\"type\":\"long\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}");
    Record record = new Record(schema);

    Schema enumSchema = new Parser().parse(
      "{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}}");
    record.put("key", new EnumSymbol(enumSchema, "ABSOLUTE"));
    record.put("value", 135L);
    assertEquals(1, data.resolveUnion(rootSchema, record));
  }

  @Test
  public void shouldTest_Pair() throws IOException {
    Pair<Integer, String> pair1 = new Pair<Integer, String>();
    pair1.setKey(102);
    pair1.setValue("one_not_two");

    Pair<Long, Double> pair2 = new Pair<Long, Double>();
    pair2.setKey(1001L);
    pair2.setValue(1001.00);

    Pair<Long, Double> pair3 = new Pair<Long, Double>();
    pair3.setKey(2001L);
    pair3.setValue(2001.00);
    TestPair2 testPair2 = new TestPair2(pair1, pair2, pair3);

    ReflectData reflectData = new ReflectData.AllowNull();
    Schema schema = reflectData.getSchema(TestPair2.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    DatumWriter<TestPair2> datumWriter = new ReflectDatumWriter<TestPair2>(schema, reflectData);
    datumWriter.write(testPair2, encoder);

    encoder.flush();
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();
    assertNotNull(bytes);

    DatumReader<TestPair2> datumReader = new ReflectDatumReader<TestPair2>(schema, schema,
      reflectData);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

    TestPair2 testPair2_DeSerialized = datumReader.read(null, decoder);
    assertNotNull(testPair2_DeSerialized);

    Pair<Integer, String> value1 = testPair2_DeSerialized.getValue1();
    assertEquals(value1.getClass(), Pair.class);
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

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    DatumWriter<TestPair3> datumWriter = new ReflectDatumWriter<TestPair3>(schema, reflectData);
    datumWriter.write(testPair3, encoder);

    encoder.flush();
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();
    assertNotNull(bytes);

    DatumReader<TestPair3> datumReader = new ReflectDatumReader<TestPair3>(schema, schema,
      reflectData);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

    TestPair3 testPair3_DeSerialized = datumReader.read(null, decoder);
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
    Map<Integer, String> map1 = new HashMap<Integer, String>();
    map1.put(1, "one");
    map1.put(2, "two");

    TestMap1 testMap1 = new TestMap1(map1);

    ReflectData reflectData = new ReflectData.AllowNull();
    Schema schema = reflectData.getSchema(TestMap1.class);
    assertNotNull(schema);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    DatumWriter<TestMap1> datumWriter = new ReflectDatumWriter<TestMap1>(schema, reflectData);
    datumWriter.write(testMap1, encoder);

    encoder.flush();
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();
    assertNotNull(bytes);

    DatumReader<TestMap1> datumReader = new ReflectDatumReader<TestMap1>(schema, schema,
      reflectData);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

    TestMap1 testMap_deSerialized = datumReader.read(null, decoder);
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

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    DatumWriter<TestMap2> datumWriter = new ReflectDatumWriter<TestMap2>(schema, reflectData);
    datumWriter.write(testMap2, encoder);

    encoder.flush();
    outputStream.close();
    byte[] bytes = outputStream.toByteArray();
    assertNotNull(bytes);

    DatumReader<TestMap2> datumReader = new ReflectDatumReader<TestMap2>(schema, schema,
      reflectData);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

    TestMap2 testMap_deSerialized = datumReader.read(null, decoder);
    assertNotNull(testMap_deSerialized);
  }

  @Test
  public void shouldDeserialize_ParamType_Generic() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"CancellationCharge\",\"namespace\":\"com.phonepe.services.models.ticketing.commons\",\"fields\":[{\"name\":\"cancellationCharge\",\"type\":[\"null\",{\"type\":\"param\",\"values\":{\"type\":\"record\",\"name\":\"PairMetricTypeLong\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"MetricType\",\"symbols\":[\"ABSOLUTE\",\"PERCENTAGE\",\"RELATIVE\"]}},{\"name\":\"value\",\"type\":\"long\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}}],\"default\":null},{\"name\":\"cancellationInterval\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"AbsoluteTimeInterval\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"from\",\"type\":\"long\"},{\"name\":\"to\",\"type\":\"long\"},{\"name\":\"type\",\"type\":[\"null\",\"MetricType\"],\"default\":null}]},{\"type\":\"record\",\"name\":\"RelativeTimeInterval\",\"namespace\":\"com.phonepe.services.models.commons\",\"fields\":[{\"name\":\"from\",\"type\":[\"null\",{\"type\":\"param\",\"values\":{\"type\":\"record\",\"name\":\"PairTimeUnitInteger\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"enum\",\"name\":\"TimeUnit\",\"namespace\":\"java.util.concurrent\",\"symbols\":[\"NANOSECONDS\",\"MICROSECONDS\",\"MILLISECONDS\",\"SECONDS\",\"MINUTES\",\"HOURS\",\"DAYS\"]}},{\"name\":\"value\",\"type\":\"int\"}],\"java-class\":\"com.phonepe.services.models.commons.Pair\"}}],\"default\":null},{\"name\":\"to\",\"type\":[\"null\",{\"type\":\"param\",\"values\":\"PairTimeUnitInteger\"}],\"default\":null},{\"name\":\"type\",\"type\":[\"null\",\"MetricType\"],\"default\":null}]}],\"default\":null},{\"name\":\"cancellationIntervalTag\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cancellationChargeTag\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"cancellationChargeTagApplicable\",\"type\":\"boolean\"},{\"name\":\"cancellationIntervalTagApplicable\",\"type\":\"boolean\"}]}";

    String data = "{\"cancellationCharge\": {\"key\": \"PERCENTAGE\", \"value\": 100}, \"cancellationInterval\": {\"from\": {\"key\": HOURS, \"value\": 4}, \"to\": {\"key\": HOURS, \"value\": 0}, \"type\": \"RELATIVE\"}, \"cancellationIntervalTag\": null, \"cancellationChargeTag\": null, \"cancellationChargeTagApplicable\": false, \"cancellationIntervalTagApplicable\": false}";
  }
}
