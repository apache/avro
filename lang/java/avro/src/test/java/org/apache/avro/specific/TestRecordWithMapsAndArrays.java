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
package org.apache.avro.specific;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@AvroGenerated
public class TestRecordWithMapsAndArrays extends SpecificRecordBase implements SpecificRecord {
  private static final long serialVersionUID = -3823801533006425147L;

  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"TestRecordWithMapsAndArrays\",\"namespace\":\"org.apache.avro.specific\",\"fields\":[{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":[]}},{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":\"long\",\"avro.java.string\":\"String\",\"default\":{}}},{\"name\":\"nested_arr\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":[]},\"default\":[]}},{\"name\":\"nested_map\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"map\",\"values\":\"long\",\"avro.java.string\":\"String\",\"default\":{}},\"avro.java.string\":\"String\",\"default\":{}}}]}");

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<TestRecordWithMapsAndArrays> ENCODER = new BinaryMessageEncoder<>(MODEL$,
      SCHEMA$);

  private static final BinaryMessageDecoder<TestRecordWithMapsAndArrays> DECODER = new BinaryMessageDecoder<>(MODEL$,
      SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * 
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<TestRecordWithMapsAndArrays> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * 
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<TestRecordWithMapsAndArrays> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the
   * specified {@link SchemaStore}.
   * 
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given
   *         SchemaStore
   */
  public static BinaryMessageDecoder<TestRecordWithMapsAndArrays> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this TestRecordWithMapsAndArrays to a ByteBuffer.
   * 
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a TestRecordWithMapsAndArrays from a ByteBuffer.
   * 
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a TestRecordWithMapsAndArrays instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into
   *                             an instance of this class
   */
  public static TestRecordWithMapsAndArrays fromByteBuffer(java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private java.util.List<String> arr;
  private java.util.Map<String, Long> map;
  private java.util.List<java.util.List<String>> nested_arr;
  private java.util.Map<String, java.util.Map<String, Long>> nested_map;

  /**
   * Default constructor. Note that this does not initialize fields to their
   * default values from the schema. If that is desired then one should use
   * <code>newBuilder()</code>.
   */
  public TestRecordWithMapsAndArrays() {
  }

  /**
   * All-args constructor.
   * 
   * @param arr        The new value for arr
   * @param map        The new value for map
   * @param nested_arr The new value for nested_arr
   * @param nested_map The new value for nested_map
   */
  public TestRecordWithMapsAndArrays(java.util.List<String> arr, java.util.Map<String, Long> map,
      java.util.List<java.util.List<String>> nested_arr,
      java.util.Map<String, java.util.Map<String, Long>> nested_map) {
    this.arr = arr;
    this.map = map;
    this.nested_arr = nested_arr;
    this.nested_map = nested_map;
  }

  @Override
  public SpecificData getSpecificData() {
    return MODEL$;
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  @Override
  public Object get(int field$) {
    switch (field$) {
    case 0:
      return arr;
    case 1:
      return map;
    case 2:
      return nested_arr;
    case 3:
      return nested_map;
    default:
      throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader. Applications should not call.
  @Override
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0:
      arr = (java.util.List<String>) value$;
      break;
    case 1:
      map = (java.util.Map<String, Long>) value$;
      break;
    case 2:
      nested_arr = (java.util.List<java.util.List<String>>) value$;
      break;
    case 3:
      nested_map = (java.util.Map<String, java.util.Map<String, Long>>) value$;
      break;
    default:
      throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'arr' field.
   * 
   * @return The value of the 'arr' field.
   */
  public java.util.List<String> getArr() {
    return arr;
  }

  /**
   * Sets the value of the 'arr' field.
   * 
   * @param value the value to set.
   */
  public void setArr(java.util.List<String> value) {
    this.arr = value;
  }

  /**
   * Gets the value of the 'map' field.
   * 
   * @return The value of the 'map' field.
   */
  public java.util.Map<String, Long> getMap() {
    return map;
  }

  /**
   * Sets the value of the 'map' field.
   * 
   * @param value the value to set.
   */
  public void setMap(java.util.Map<String, Long> value) {
    this.map = value;
  }

  /**
   * Gets the value of the 'nested_arr' field.
   * 
   * @return The value of the 'nested_arr' field.
   */
  public java.util.List<java.util.List<String>> getNestedArr() {
    return nested_arr;
  }

  /**
   * Sets the value of the 'nested_arr' field.
   * 
   * @param value the value to set.
   */
  public void setNestedArr(java.util.List<java.util.List<String>> value) {
    this.nested_arr = value;
  }

  /**
   * Gets the value of the 'nested_map' field.
   * 
   * @return The value of the 'nested_map' field.
   */
  public java.util.Map<String, java.util.Map<String, Long>> getNestedMap() {
    return nested_map;
  }

  /**
   * Sets the value of the 'nested_map' field.
   * 
   * @param value the value to set.
   */
  public void setNestedMap(java.util.Map<String, java.util.Map<String, Long>> value) {
    this.nested_map = value;
  }

  /**
   * Creates a new TestRecordWithMapsAndArrays RecordBuilder.
   * 
   * @return A new TestRecordWithMapsAndArrays RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new TestRecordWithMapsAndArrays RecordBuilder by copying an
   * existing Builder.
   * 
   * @param other The existing builder to copy.
   * @return A new TestRecordWithMapsAndArrays RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new TestRecordWithMapsAndArrays RecordBuilder by copying an
   * existing TestRecordWithMapsAndArrays instance.
   * 
   * @param other The existing instance to copy.
   * @return A new TestRecordWithMapsAndArrays RecordBuilder
   */
  public static Builder newBuilder(TestRecordWithMapsAndArrays other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for TestRecordWithMapsAndArrays instances.
   */
  @AvroGenerated
  public static class Builder extends SpecificRecordBuilderBase<TestRecordWithMapsAndArrays>
      implements org.apache.avro.data.RecordBuilder<TestRecordWithMapsAndArrays> {

    private java.util.List<String> arr;
    private java.util.Map<String, Long> map;
    private java.util.List<java.util.List<String>> nested_arr;
    private java.util.Map<String, java.util.Map<String, Long>> nested_map;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * 
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.arr)) {
        this.arr = data().deepCopy(fields()[0].schema(), other.arr);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.map)) {
        this.map = data().deepCopy(fields()[1].schema(), other.map);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.nested_arr)) {
        this.nested_arr = data().deepCopy(fields()[2].schema(), other.nested_arr);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.nested_map)) {
        this.nested_map = data().deepCopy(fields()[3].schema(), other.nested_map);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing TestRecordWithMapsAndArrays instance
     * 
     * @param other The existing instance to copy.
     */
    private Builder(TestRecordWithMapsAndArrays other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.arr)) {
        this.arr = data().deepCopy(fields()[0].schema(), other.arr);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.map)) {
        this.map = data().deepCopy(fields()[1].schema(), other.map);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.nested_arr)) {
        this.nested_arr = data().deepCopy(fields()[2].schema(), other.nested_arr);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.nested_map)) {
        this.nested_map = data().deepCopy(fields()[3].schema(), other.nested_map);
        fieldSetFlags()[3] = true;
      }
    }

    /**
     * Gets the value of the 'arr' field.
     * 
     * @return The value.
     */
    public java.util.List<String> getArr() {
      return arr;
    }

    /**
     * Sets the value of the 'arr' field.
     * 
     * @param value The value of 'arr'.
     * @return This builder.
     */
    public Builder setArr(java.util.List<String> value) {
      validate(fields()[0], value);
      this.arr = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
     * Checks whether the 'arr' field has been set.
     * 
     * @return True if the 'arr' field has been set, false otherwise.
     */
    public boolean hasArr() {
      return fieldSetFlags()[0];
    }

    /**
     * Clears the value of the 'arr' field.
     * 
     * @return This builder.
     */
    public Builder clearArr() {
      arr = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
     * Gets the value of the 'map' field.
     * 
     * @return The value.
     */
    public java.util.Map<String, Long> getMap() {
      return map;
    }

    /**
     * Sets the value of the 'map' field.
     * 
     * @param value The value of 'map'.
     * @return This builder.
     */
    public Builder setMap(java.util.Map<String, Long> value) {
      validate(fields()[1], value);
      this.map = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
     * Checks whether the 'map' field has been set.
     * 
     * @return True if the 'map' field has been set, false otherwise.
     */
    public boolean hasMap() {
      return fieldSetFlags()[1];
    }

    /**
     * Clears the value of the 'map' field.
     * 
     * @return This builder.
     */
    public Builder clearMap() {
      map = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
     * Gets the value of the 'nested_arr' field.
     * 
     * @return The value.
     */
    public java.util.List<java.util.List<String>> getNestedArr() {
      return nested_arr;
    }

    /**
     * Sets the value of the 'nested_arr' field.
     * 
     * @param value The value of 'nested_arr'.
     * @return This builder.
     */
    public Builder setNestedArr(java.util.List<java.util.List<String>> value) {
      validate(fields()[2], value);
      this.nested_arr = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
     * Checks whether the 'nested_arr' field has been set.
     * 
     * @return True if the 'nested_arr' field has been set, false otherwise.
     */
    public boolean hasNestedArr() {
      return fieldSetFlags()[2];
    }

    /**
     * Clears the value of the 'nested_arr' field.
     * 
     * @return This builder.
     */
    public Builder clearNestedArr() {
      nested_arr = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
     * Gets the value of the 'nested_map' field.
     * 
     * @return The value.
     */
    public java.util.Map<String, java.util.Map<String, Long>> getNestedMap() {
      return nested_map;
    }

    /**
     * Sets the value of the 'nested_map' field.
     * 
     * @param value The value of 'nested_map'.
     * @return This builder.
     */
    public Builder setNestedMap(java.util.Map<String, java.util.Map<String, Long>> value) {
      validate(fields()[3], value);
      this.nested_map = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
     * Checks whether the 'nested_map' field has been set.
     * 
     * @return True if the 'nested_map' field has been set, false otherwise.
     */
    public boolean hasNestedMap() {
      return fieldSetFlags()[3];
    }

    /**
     * Clears the value of the 'nested_map' field.
     * 
     * @return This builder.
     */
    public Builder clearNestedMap() {
      nested_map = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TestRecordWithMapsAndArrays build() {
      try {
        TestRecordWithMapsAndArrays record = new TestRecordWithMapsAndArrays();
        record.arr = fieldSetFlags()[0] ? this.arr : (java.util.List<String>) defaultValue(fields()[0]);
        record.map = fieldSetFlags()[1] ? this.map : (java.util.Map<String, Long>) defaultValue(fields()[1]);
        record.nested_arr = fieldSetFlags()[2] ? this.nested_arr
            : (java.util.List<java.util.List<String>>) defaultValue(fields()[2]);
        record.nested_map = fieldSetFlags()[3] ? this.nested_map
            : (java.util.Map<String, java.util.Map<String, Long>>) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<TestRecordWithMapsAndArrays> WRITER$ = (org.apache.avro.io.DatumWriter<TestRecordWithMapsAndArrays>) MODEL$
      .createDatumWriter(SCHEMA$);

  @Override
  public void writeExternal(java.io.ObjectOutput out) throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<TestRecordWithMapsAndArrays> READER$ = (org.apache.avro.io.DatumReader<TestRecordWithMapsAndArrays>) MODEL$
      .createDatumReader(SCHEMA$);

  @Override
  public void readExternal(java.io.ObjectInput in) throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override
  protected boolean hasCustomCoders() {
    return true;
  }

  @Override
  public void customEncode(org.apache.avro.io.Encoder out) throws java.io.IOException {
    long size0 = this.arr.size();
    out.writeArrayStart();
    out.setItemCount(size0);
    long actualSize0 = 0;
    for (String e0 : this.arr) {
      actualSize0++;
      out.startItem();
      out.writeString(e0);
    }
    out.writeArrayEnd();
    if (actualSize0 != size0)
      throw new java.util.ConcurrentModificationException(
          "Array-size written was " + size0 + ", but element count was " + actualSize0 + ".");

    long size1 = this.map.size();
    out.writeMapStart();
    out.setItemCount(size1);
    long actualSize1 = 0;
    for (java.util.Map.Entry<String, Long> e1 : this.map.entrySet()) {
      actualSize1++;
      out.startItem();
      out.writeString(e1.getKey());
      Long v1 = e1.getValue();
      out.writeLong(v1);
    }
    out.writeMapEnd();
    if (actualSize1 != size1)
      throw new java.util.ConcurrentModificationException(
          "Map-size written was " + size1 + ", but element count was " + actualSize1 + ".");

    long size2 = this.nested_arr.size();
    out.writeArrayStart();
    out.setItemCount(size2);
    long actualSize2 = 0;
    for (java.util.List<String> e2 : this.nested_arr) {
      actualSize2++;
      out.startItem();
      long size3 = e2.size();
      out.writeArrayStart();
      out.setItemCount(size3);
      long actualSize3 = 0;
      for (String e3 : e2) {
        actualSize3++;
        out.startItem();
        out.writeString(e3);
      }
      out.writeArrayEnd();
      if (actualSize3 != size3)
        throw new java.util.ConcurrentModificationException(
            "Array-size written was " + size3 + ", but element count was " + actualSize3 + ".");
    }
    out.writeArrayEnd();
    if (actualSize2 != size2)
      throw new java.util.ConcurrentModificationException(
          "Array-size written was " + size2 + ", but element count was " + actualSize2 + ".");

    long size4 = this.nested_map.size();
    out.writeMapStart();
    out.setItemCount(size4);
    long actualSize4 = 0;
    for (java.util.Map.Entry<String, java.util.Map<String, Long>> e4 : this.nested_map.entrySet()) {
      actualSize4++;
      out.startItem();
      out.writeString(e4.getKey());
      java.util.Map<String, Long> v4 = e4.getValue();
      long size5 = v4.size();
      out.writeMapStart();
      out.setItemCount(size5);
      long actualSize5 = 0;
      for (java.util.Map.Entry<String, Long> e5 : v4.entrySet()) {
        actualSize5++;
        out.startItem();
        out.writeString(e5.getKey());
        Long v5 = e5.getValue();
        out.writeLong(v5);
      }
      out.writeMapEnd();
      if (actualSize5 != size5)
        throw new java.util.ConcurrentModificationException(
            "Map-size written was " + size5 + ", but element count was " + actualSize5 + ".");
    }
    out.writeMapEnd();
    if (actualSize4 != size4)
      throw new java.util.ConcurrentModificationException(
          "Map-size written was " + size4 + ", but element count was " + actualSize4 + ".");

  }

  @Override
  public void customDecode(org.apache.avro.io.ResolvingDecoder in) throws java.io.IOException {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      long size0 = in.readArrayStart();
      java.util.List<String> a0 = this.arr;
      if (a0 == null) {
        a0 = new SpecificData.Array<String>((int) size0, SCHEMA$.getField("arr").schema());
        this.arr = a0;
      } else
        a0.clear();
      SpecificData.Array<String> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<String>) a0 : null);
      for (; 0 < size0; size0 = in.arrayNext()) {
        for (; size0 != 0; size0--) {
          String e0 = (ga0 != null ? ga0.peek() : null);
          e0 = in.readString();
          a0.add(e0);
        }
      }

      long size1 = in.readMapStart();
      java.util.Map<String, Long> m1 = this.map; // Need fresh name due to limitation of macro system
      if (m1 == null) {
        m1 = new java.util.HashMap<String, Long>((int) size1);
        this.map = m1;
      } else
        m1.clear();
      for (; 0 < size1; size1 = in.mapNext()) {
        for (; size1 != 0; size1--) {
          String k1 = null;
          k1 = in.readString();
          Long v1 = null;
          v1 = in.readLong();
          m1.put(k1, v1);
        }
      }

      long size2 = in.readArrayStart();
      java.util.List<java.util.List<String>> a2 = this.nested_arr;
      if (a2 == null) {
        a2 = new SpecificData.Array<java.util.List<String>>((int) size2, SCHEMA$.getField("nested_arr").schema());
        this.nested_arr = a2;
      } else
        a2.clear();
      SpecificData.Array<java.util.List<String>> ga2 = (a2 instanceof SpecificData.Array
          ? (SpecificData.Array<java.util.List<String>>) a2
          : null);
      for (; 0 < size2; size2 = in.arrayNext()) {
        for (; size2 != 0; size2--) {
          java.util.List<String> e2 = (ga2 != null ? ga2.peek() : null);
          long size3 = in.readArrayStart();
          java.util.List<String> a3 = e2;
          if (a3 == null) {
            a3 = new SpecificData.Array<String>((int) size3, SCHEMA$.getField("nested_arr").schema().getElementType());
            e2 = a3;
          } else
            a3.clear();
          SpecificData.Array<String> ga3 = (a3 instanceof SpecificData.Array ? (SpecificData.Array<String>) a3 : null);
          for (; 0 < size3; size3 = in.arrayNext()) {
            for (; size3 != 0; size3--) {
              String e3 = (ga3 != null ? ga3.peek() : null);
              e3 = in.readString();
              a3.add(e3);
            }
          }
          a2.add(e2);
        }
      }

      long size4 = in.readMapStart();
      java.util.Map<String, java.util.Map<String, Long>> m4 = this.nested_map; // Need fresh name due to limitation of
                                                                               // macro system
      if (m4 == null) {
        m4 = new java.util.HashMap<String, java.util.Map<String, Long>>((int) size4);
        this.nested_map = m4;
      } else
        m4.clear();
      for (; 0 < size4; size4 = in.mapNext()) {
        for (; size4 != 0; size4--) {
          String k4 = null;
          k4 = in.readString();
          java.util.Map<String, Long> v4 = null;
          long size5 = in.readMapStart();
          java.util.Map<String, Long> m5 = v4; // Need fresh name due to limitation of macro system
          if (m5 == null) {
            m5 = new java.util.HashMap<String, Long>((int) size5);
            v4 = m5;
          } else
            m5.clear();
          for (; 0 < size5; size5 = in.mapNext()) {
            for (; size5 != 0; size5--) {
              String k5 = null;
              k5 = in.readString();
              Long v5 = null;
              v5 = in.readLong();
              m5.put(k5, v5);
            }
          }
          m4.put(k4, v4);
        }
      }

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          long size0 = in.readArrayStart();
          java.util.List<String> a0 = this.arr;
          if (a0 == null) {
            a0 = new SpecificData.Array<String>((int) size0, SCHEMA$.getField("arr").schema());
            this.arr = a0;
          } else
            a0.clear();
          SpecificData.Array<String> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<String>) a0 : null);
          for (; 0 < size0; size0 = in.arrayNext()) {
            for (; size0 != 0; size0--) {
              String e0 = (ga0 != null ? ga0.peek() : null);
              e0 = in.readString();
              a0.add(e0);
            }
          }
          break;

        case 1:
          long size1 = in.readMapStart();
          java.util.Map<String, Long> m1 = this.map; // Need fresh name due to limitation of macro system
          if (m1 == null) {
            m1 = new java.util.HashMap<String, Long>((int) size1);
            this.map = m1;
          } else
            m1.clear();
          for (; 0 < size1; size1 = in.mapNext()) {
            for (; size1 != 0; size1--) {
              String k1 = null;
              k1 = in.readString();
              Long v1 = null;
              v1 = in.readLong();
              m1.put(k1, v1);
            }
          }
          break;

        case 2:
          long size2 = in.readArrayStart();
          java.util.List<java.util.List<String>> a2 = this.nested_arr;
          if (a2 == null) {
            a2 = new SpecificData.Array<java.util.List<String>>((int) size2, SCHEMA$.getField("nested_arr").schema());
            this.nested_arr = a2;
          } else
            a2.clear();
          SpecificData.Array<java.util.List<String>> ga2 = (a2 instanceof SpecificData.Array
              ? (SpecificData.Array<java.util.List<String>>) a2
              : null);
          for (; 0 < size2; size2 = in.arrayNext()) {
            for (; size2 != 0; size2--) {
              java.util.List<String> e2 = (ga2 != null ? ga2.peek() : null);
              long size3 = in.readArrayStart();
              java.util.List<String> a3 = e2;
              if (a3 == null) {
                a3 = new SpecificData.Array<String>((int) size3,
                    SCHEMA$.getField("nested_arr").schema().getElementType());
                e2 = a3;
              } else
                a3.clear();
              SpecificData.Array<String> ga3 = (a3 instanceof SpecificData.Array ? (SpecificData.Array<String>) a3
                  : null);
              for (; 0 < size3; size3 = in.arrayNext()) {
                for (; size3 != 0; size3--) {
                  String e3 = (ga3 != null ? ga3.peek() : null);
                  e3 = in.readString();
                  a3.add(e3);
                }
              }
              a2.add(e2);
            }
          }
          break;

        case 3:
          long size4 = in.readMapStart();
          java.util.Map<String, java.util.Map<String, Long>> m4 = this.nested_map; // Need fresh name due to limitation
                                                                                   // of macro system
          if (m4 == null) {
            m4 = new java.util.HashMap<String, java.util.Map<String, Long>>((int) size4);
            this.nested_map = m4;
          } else
            m4.clear();
          for (; 0 < size4; size4 = in.mapNext()) {
            for (; size4 != 0; size4--) {
              String k4 = null;
              k4 = in.readString();
              java.util.Map<String, Long> v4 = null;
              long size5 = in.readMapStart();
              java.util.Map<String, Long> m5 = v4; // Need fresh name due to limitation of macro system
              if (m5 == null) {
                m5 = new java.util.HashMap<String, Long>((int) size5);
                v4 = m5;
              } else
                m5.clear();
              for (; 0 < size5; size5 = in.mapNext()) {
                for (; size5 != 0; size5--) {
                  String k5 = null;
                  k5 = in.readString();
                  Long v5 = null;
                  v5 = in.readLong();
                  m5.put(k5, v5);
                }
              }
              m4.put(k4, v4);
            }
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}
