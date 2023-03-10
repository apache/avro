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
import java.util.Optional;

import org.apache.avro.Schema;
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
  void read_PojoWithList() throws IOException {
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
  void read_PojoWithArray() throws IOException {
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

  @Test
  public void testRead_PojoWithOptional() throws IOException {
    PojoWithOptional pojoWithOptional = new PojoWithOptional();
    pojoWithOptional.setId(42);
    pojoWithOptional.setRelatedId(Optional.of(13));

    byte[] serializedBytes = serializeWithReflectDatumWriter(pojoWithOptional, PojoWithOptional.class);

    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
    ReflectDatumReader<PojoWithOptional> reflectDatumReader = new ReflectDatumReader<>(PojoWithOptional.class);

    PojoWithOptional deserialized = new PojoWithOptional();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(pojoWithOptional, deserialized);
  }

  @Test
  public void testRead_PojoWithEmptyOptional() throws IOException {
    PojoWithOptional pojoWithOptional = new PojoWithOptional();
    pojoWithOptional.setId(42);
    pojoWithOptional.setRelatedId(Optional.empty());

    byte[] serializedBytes = serializeWithReflectDatumWriter(pojoWithOptional, PojoWithOptional.class);

    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);
    ReflectDatumReader<PojoWithOptional> reflectDatumReader = new ReflectDatumReader<>(PojoWithOptional.class);

    PojoWithOptional deserialized = new PojoWithOptional();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(pojoWithOptional, deserialized);
  }

  @Test
  public void testRead_PojoWithNullableAnnotation() throws IOException {
    PojoWithBasicTypeNullableAnnotationV1 v1Pojo = new PojoWithBasicTypeNullableAnnotationV1();
    int idValue = 1;
    v1Pojo.setId(idValue);
    byte[] serializedBytes = serializeWithReflectDatumWriter(v1Pojo, PojoWithBasicTypeNullableAnnotationV1.class);
    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedBytes, null);

    ReflectData reflectData = ReflectData.get();
    Schema schemaV1 = reflectData.getSchema(PojoWithBasicTypeNullableAnnotationV1.class);
    Schema schemaV2 = reflectData.getSchema(PojoWithBasicTypeNullableAnnotationV2.class);

    ReflectDatumReader<PojoWithBasicTypeNullableAnnotationV2> reflectDatumReader = new ReflectDatumReader<>(schemaV1,
        schemaV2);

    PojoWithBasicTypeNullableAnnotationV2 v2Pojo = new PojoWithBasicTypeNullableAnnotationV2();
    reflectDatumReader.read(v2Pojo, decoder);

    assertEquals(v1Pojo.id, v2Pojo.id);
    assertEquals(v2Pojo.id, idValue);
    assertEquals(v2Pojo.intId, FieldAccess.INT_DEFAULT_VALUE);
    assertEquals(v2Pojo.floatId, FieldAccess.FLOAT_DEFAULT_VALUE);
    assertEquals(v2Pojo.shortId, FieldAccess.SHORT_DEFAULT_VALUE);
    assertEquals(v2Pojo.byteId, FieldAccess.BYTE_DEFAULT_VALUE);
    assertEquals(v2Pojo.booleanId, FieldAccess.BOOLEAN_DEFAULT_VALUE);
    assertEquals(v2Pojo.charId, FieldAccess.CHAR_DEFAULT_VALUE);
    assertEquals(v2Pojo.longId, FieldAccess.LONG_DEFAULT_VALUE);
    assertEquals(v2Pojo.doubleId, FieldAccess.DOUBLE_DEFAULT_VALUE);
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

  public static class PojoWithOptional {
    private int id;

    private Optional<Integer> relatedId;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Optional<Integer> getRelatedId() {
      return relatedId;
    }

    public void setRelatedId(Optional<Integer> relatedId) {
      this.relatedId = relatedId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
      result = prime * result + ((relatedId == null) ? 0 : relatedId.hashCode());
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
      PojoWithOptional other = (PojoWithOptional) obj;
      if (id != other.id)
        return false;
      if (relatedId == null) {
        return other.relatedId == null;
      } else
        return relatedId.equals(other.relatedId);
    }
  }

  public static class PojoWithBasicTypeNullableAnnotationV1 {

    private int id;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
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
      PojoWithBasicTypeNullableAnnotationV1 other = (PojoWithBasicTypeNullableAnnotationV1) obj;
      return id == other.id;
    }
  }

  public static class PojoWithBasicTypeNullableAnnotationV2 {

    private int id;

    @Nullable
    private int intId;

    @Nullable
    private float floatId;

    @Nullable
    private short shortId;

    @Nullable
    private byte byteId;

    @Nullable
    private boolean booleanId;

    @Nullable
    private char charId;

    @Nullable
    private long longId;

    @Nullable
    private double doubleId;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getIntId() {
      return intId;
    }

    public void setIntId(int intId) {
      this.intId = intId;
    }

    public float getFloatId() {
      return floatId;
    }

    public void setFloatId(float floatId) {
      this.floatId = floatId;
    }

    public short getShortId() {
      return shortId;
    }

    public void setShortId(short shortId) {
      this.shortId = shortId;
    }

    public byte getByteId() {
      return byteId;
    }

    public void setByteId(byte byteId) {
      this.byteId = byteId;
    }

    public boolean isBooleanId() {
      return booleanId;
    }

    public void setBooleanId(boolean booleanId) {
      this.booleanId = booleanId;
    }

    public char getCharId() {
      return charId;
    }

    public void setCharId(char charId) {
      this.charId = charId;
    }

    public long getLongId() {
      return longId;
    }

    public void setLongId(long longId) {
      this.longId = longId;
    }

    public double getDoubleId() {
      return doubleId;
    }

    public void setDoubleId(double doubleId) {
      this.doubleId = doubleId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      long temp;
      int result = 1;
      result = prime * result + id;
      result = prime * result + intId;
      result = prime * result + (floatId != 0.0f ? Float.floatToIntBits(floatId) : 0);
      result = prime * result + (int) shortId;
      result = prime * result + (int) byteId;
      result = prime * result + (booleanId ? 1 : 0);
      result = prime * result + (int) charId;
      result = prime * result + (int) (longId ^ (longId >>> 32));
      temp = Double.doubleToLongBits(doubleId);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      PojoWithBasicTypeNullableAnnotationV2 that = (PojoWithBasicTypeNullableAnnotationV2) o;
      if (id != that.id)
        return false;
      if (intId != that.intId)
        return false;
      if (Float.compare(that.floatId, floatId) != 0)
        return false;
      if (shortId != that.shortId)
        return false;
      if (byteId != that.byteId)
        return false;
      if (booleanId != that.booleanId)
        return false;
      if (charId != that.charId)
        return false;
      if (longId != that.longId)
        return false;
      return Double.compare(that.doubleId, doubleId) == 0;
    }
  }
}
