package org.apache.avro.reflect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.FooBarSpecificRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.TestSpecificDatumReader;
import org.junit.Test;

public class TestReflectDatumReader {

  @Test
  public void testRead_SpecificDataRecord() throws IOException {
    FooBarSpecificRecord specificRecord = FooBarSpecificRecord.newBuilder().setId(42)
        .setRelatedids(Arrays.asList(1, 2, 3)).build();
    byte[] specificRecordBytes = TestSpecificDatumReader.serializeRecord(specificRecord);

    Decoder decoder = DecoderFactory.get().binaryDecoder(specificRecordBytes, null);
    ReflectDatumReader<FooBarSpecificRecord> reflectDatumReader = new ReflectDatumReader<FooBarSpecificRecord>(
        FooBarSpecificRecord.class);

    FooBarSpecificRecord deserialized = new FooBarSpecificRecord();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(specificRecord, deserialized);
  }

  private static <T> byte[] serializeWithReflectDatumWriter(T toSerialize, Class<T> toSerializeClass)
      throws IOException {
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<T>(toSerializeClass);
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
    ReflectDatumReader<PojoWithList> reflectDatumReader = new ReflectDatumReader<PojoWithList>(
        PojoWithList.class);

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
    ReflectDatumReader<PojoWithArray> reflectDatumReader = new ReflectDatumReader<PojoWithArray>(
        PojoWithArray.class);

    PojoWithArray deserialized = new PojoWithArray();
    reflectDatumReader.read(deserialized, decoder);

    assertEquals(pojoWithArray, deserialized);
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
        if (other.relatedIds != null)
          return false;
      } else if (!relatedIds.equals(other.relatedIds))
        return false;
      return true;
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
      if (!Arrays.equals(relatedIds, other.relatedIds))
        return false;
      return true;
    }

  }
}
