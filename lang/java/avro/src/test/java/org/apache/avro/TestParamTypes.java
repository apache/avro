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


  private static class TestPair2 {

    private Pair<Integer, String> value;

    public TestPair2(Pair<Integer, String> value) {
      this.value = value;
    }

    public TestPair2() {
    }

    public Pair<Integer, String> getValue() {
      return value;
    }

    public void setValue(Pair<Integer, String> value) {
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
      TestPair2 testPair2 = (TestPair2) o;
      return Objects.equals(getValue(), testPair2.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue());
    }
  }

  private static class TestMap2 {

    private Map<Integer, String> value;

    public TestMap2(Map<Integer, String> value) {
      this.value = value;
    }

    public Map<Integer, String> getValue() {
      return value;
    }

    public void setValue(Map<Integer, String> value) {
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
      TestMap2 testMap2 = (TestMap2) o;
      return Objects.equals(getValue(), testMap2.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue());
    }
  }

  @Test
  public void shouldTest_Pair() throws IOException {
    Pair<Integer, String> pair = new Pair<Integer, String>();
    pair.setKey(102);
    pair.setValue("one_not_two");
    TestPair2 testPair2 = new TestPair2(pair);

    ReflectData reflectData = new ReflectData.AllowNull();
    //ReflectData reflectData = new ReflectData();
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
  }

  @Test
  public void shouldTest_Map() throws IOException {
    Map<Integer, String> sampleMap = new HashMap<Integer, String>();
    sampleMap.put(1, "one");
    sampleMap.put(2, "two");
    assertEquals(2, sampleMap.size());

    TestMap2 testMap2 = new TestMap2(sampleMap);

    ReflectData reflectData = new ReflectData.AllowNull();
    //ReflectData reflectData = new ReflectData();
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
  }
}
