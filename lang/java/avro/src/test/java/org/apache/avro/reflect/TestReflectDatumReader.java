/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.reflect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

public class TestReflectDatumReader {

  private static <T> byte[] serializeWithReflectDatumWriter(T toSerialize, Class<T> toSerializeClass)
      throws IOException {
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(toSerializeClass);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
    datumWriter.write(toSerialize, encoder);
    encoder.flush();
    return byteArrayOutputStream.toByteArray();
  }

  @Test
  public void testRead_PojoWithList() throws IOException {
    PojoWithList pojoWithList = new PojoWithList();
    pojoWithList.setId(42);
    pojoWithList.setRelatedIds(Arrays.asList(1, 2, 3));

    byte[] serializedBytes = serializeWithReflectDatumWriter(pojoWithList, PojoWithList.class);

    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
    ReflectDatumReader<PojoWithList> reflectDatumReader = new ReflectDatumReader<>(PojoWithList.class);

    PojoWithList deserialized = new PojoWithList();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(pojoWithList, deserialized);

  }

  @Test
  public void testRead_PojoWithArray() throws IOException {
    PojoWithArray pojoWithArray = new PojoWithArray();
    pojoWithArray.setId(42);
    pojoWithArray.setRelatedIds(new int[] { 1, 2, 3 });

    byte[] serializedBytes = serializeWithReflectDatumWriter(pojoWithArray, PojoWithArray.class);

    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
    ReflectDatumReader<PojoWithArray> reflectDatumReader = new ReflectDatumReader<>(PojoWithArray.class);

    PojoWithArray deserialized = new PojoWithArray();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(pojoWithArray, deserialized);
  }

  @Test
  public void testRead_PojoWithSet() throws IOException {
    PojoWithSet pojoWithSet = new PojoWithSet();
    pojoWithSet.setId(42);
    Set<Integer> relatedIds = new HashSet<>();
    relatedIds.add(1);
    relatedIds.add(2);
    relatedIds.add(3);
    pojoWithSet.setRelatedIds(relatedIds);

    byte[] serializedBytes = serializeWithReflectDatumWriter(pojoWithSet, PojoWithSet.class);

    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
    ReflectDatumReader<PojoWithSet> reflectDatumReader = new ReflectDatumReader<>(PojoWithSet.class);

    PojoWithSet deserialized = new PojoWithSet();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(pojoWithSet, deserialized);

  }

  @Test
  public void testRead_PojoWithMap() throws IOException {
    PojoWithMap pojoWithMap = new PojoWithMap();
    pojoWithMap.setId(42);
    Map<Integer, Integer> relatedIds = new HashMap<>();
    relatedIds.put(1, 11);
    relatedIds.put(2, 22);
    relatedIds.put(3, 33);
    pojoWithMap.setRelatedIds(relatedIds);

    byte[] serializedBytes = serializeWithReflectDatumWriter(pojoWithMap, PojoWithMap.class);

    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
    ReflectDatumReader<PojoWithMap> reflectDatumReader = new ReflectDatumReader<>(PojoWithMap.class);

    PojoWithMap deserialized = new PojoWithMap();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(pojoWithMap, deserialized);
  }

  public static class PojoWithList {
    private int id;
    private List<Integer> relatedIds;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public List<Integer> getRelatedIds() {
      return relatedIds;
    }

    public void setRelatedIds(List<Integer> relatedIds) {
      this.relatedIds = relatedIds;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
      result = prime * result + ((relatedIds == null) ? 0 : relatedIds.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PojoWithList other = (PojoWithList) obj;
      if (id != other.id)
        return false;
      if (relatedIds == null) {
        return other.relatedIds == null;
      } else
        return relatedIds.equals(other.relatedIds);
    }
  }

  public static class PojoWithArray {
    private int id;
    private int[] relatedIds;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int[] getRelatedIds() {
      return relatedIds;
    }

    public void setRelatedIds(int[] relatedIds) {
      this.relatedIds = relatedIds;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
      result = prime * result + Arrays.hashCode(relatedIds);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PojoWithArray other = (PojoWithArray) obj;
      if (id != other.id)
        return false;
      return Arrays.equals(relatedIds, other.relatedIds);
    }
  }

  public static class PojoWithSet {
    private int id;
    private Set<Integer> relatedIds;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Set<Integer> getRelatedIds() {
      return relatedIds;
    }

    public void setRelatedIds(Set<Integer> relatedIds) {
      this.relatedIds = relatedIds;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
      result = prime * result + ((relatedIds == null) ? 0 : relatedIds.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PojoWithSet other = (PojoWithSet) obj;
      if (id != other.id)
        return false;
      if (relatedIds == null) {
        return other.relatedIds == null;
      } else
        return relatedIds.equals(other.relatedIds);
    }
  }

  public static class PojoWithMap {
    private int id;
    private Map<Integer, Integer> relatedIds;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Map<Integer, Integer> getRelatedIds() {
      return relatedIds;
    }

    public void setRelatedIds(Map<Integer, Integer> relatedIds) {
      this.relatedIds = relatedIds;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
      result = prime * result + ((relatedIds == null) ? 0 : relatedIds.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      PojoWithMap other = (PojoWithMap) obj;
      if (id != other.id)
        return false;
      if (relatedIds == null) {
        return other.relatedIds == null;
      } else
        return relatedIds.equals(other.relatedIds);
    }
  }
}
