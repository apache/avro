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

@org.apache.avro.specific.AvroGenerated
public class TestRecordWithMapsAndArrays extends org.apache.avro.specific.SpecificRecordBase
    implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3113266652594662627L;

  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"TestRecordWithMapsAndArrays\",\"namespace\":\"org.apache.avro.specific\",\"fields\":[{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":[]}},{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":\"long\",\"avro.java.string\":\"String\",\"default\":{}}}]}");

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

  private java.util.List<java.lang.String> arr;
  private java.util.Map<java.lang.String, java.lang.Long> map;

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
   * @param arr The new value for arr
   * @param map The new value for map
   */
  public TestRecordWithMapsAndArrays(java.util.List<java.lang.String> arr,
      java.util.Map<java.lang.String, java.lang.Long> map) {
    this.arr = arr;
    this.map = map;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() {
    return MODEL$;
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0:
      return arr;
    case 1:
      return map;
    default:
      throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader. Applications should not call.
  @Override
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0:
      arr = (java.util.List<java.lang.String>) value$;
      break;
    case 1:
      map = (java.util.Map<java.lang.String, java.lang.Long>) value$;
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
  public java.util.List<java.lang.String> getArr() {
    return arr;
  }

  /**
   * Sets the value of the 'arr' field.
   *
   * @param value the value to set.
   */
  public void setArr(java.util.List<java.lang.String> value) {
    this.arr = value;
  }

  /**
   * Gets the value of the 'map' field.
   *
   * @return The value of the 'map' field.
   */
  public java.util.Map<java.lang.String, java.lang.Long> getMap() {
    return map;
  }

  /**
   * Sets the value of the 'map' field.
   *
   * @param value the value to set.
   */
  public void setMap(java.util.Map<java.lang.String, java.lang.Long> value) {
    this.map = value;
  }

  /**
   * Creates a new TestRecordWithMapsAndArrays RecordBuilder.
   *
   * @return A new TestRecordWithMapsAndArrays RecordBuilder
   */
  public static org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder newBuilder() {
    return new org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder();
  }

  /**
   * Creates a new TestRecordWithMapsAndArrays RecordBuilder by copying an
   * existing Builder.
   *
   * @param other The existing builder to copy.
   * @return A new TestRecordWithMapsAndArrays RecordBuilder
   */
  public static org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder newBuilder(
      org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder other) {
    if (other == null) {
      return new org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder();
    } else {
      return new org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder(other);
    }
  }

  /**
   * Creates a new TestRecordWithMapsAndArrays RecordBuilder by copying an
   * existing TestRecordWithMapsAndArrays instance.
   *
   * @param other The existing instance to copy.
   * @return A new TestRecordWithMapsAndArrays RecordBuilder
   */
  public static org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder newBuilder(
      org.apache.avro.specific.TestRecordWithMapsAndArrays other) {
    if (other == null) {
      return new org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder();
    } else {
      return new org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder(other);
    }
  }

  /**
   * RecordBuilder for TestRecordWithMapsAndArrays instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TestRecordWithMapsAndArrays>
      implements org.apache.avro.data.RecordBuilder<TestRecordWithMapsAndArrays> {

    private java.util.List<java.lang.String> arr;
    private java.util.Map<java.lang.String, java.lang.Long> map;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     *
     * @param other The existing Builder to copy.
     */
    private Builder(org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.arr)) {
        this.arr = data().deepCopy(fields()[0].schema(), other.arr);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.map)) {
        this.map = data().deepCopy(fields()[1].schema(), other.map);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing TestRecordWithMapsAndArrays instance
     *
     * @param other The existing instance to copy.
     */
    private Builder(org.apache.avro.specific.TestRecordWithMapsAndArrays other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.arr)) {
        this.arr = data().deepCopy(fields()[0].schema(), other.arr);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.map)) {
        this.map = data().deepCopy(fields()[1].schema(), other.map);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Gets the value of the 'arr' field.
     *
     * @return The value.
     */
    public java.util.List<java.lang.String> getArr() {
      return arr;
    }

    /**
     * Sets the value of the 'arr' field.
     *
     * @param value The value of 'arr'.
     * @return This builder.
     */
    public org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder setArr(java.util.List<java.lang.String> value) {
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
    public org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder clearArr() {
      arr = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
     * Gets the value of the 'map' field.
     *
     * @return The value.
     */
    public java.util.Map<java.lang.String, java.lang.Long> getMap() {
      return map;
    }

    /**
     * Sets the value of the 'map' field.
     *
     * @param value The value of 'map'.
     * @return This builder.
     */
    public org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder setMap(
        java.util.Map<java.lang.String, java.lang.Long> value) {
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
    public org.apache.avro.specific.TestRecordWithMapsAndArrays.Builder clearMap() {
      map = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TestRecordWithMapsAndArrays build() {
      try {
        TestRecordWithMapsAndArrays record = new TestRecordWithMapsAndArrays();
        record.arr = fieldSetFlags()[0] ? this.arr : (java.util.List<java.lang.String>) defaultValue(fields()[0]);
        record.map = fieldSetFlags()[1] ? this.map
            : (java.util.Map<java.lang.String, java.lang.Long>) defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
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
    for (java.lang.String e0 : this.arr) {
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
    for (java.util.Map.Entry<java.lang.String, java.lang.Long> e1 : this.map.entrySet()) {
      actualSize1++;
      out.startItem();
      out.writeString(e1.getKey());
      java.lang.Long v1 = e1.getValue();
      out.writeLong(v1);
    }
    out.writeMapEnd();
    if (actualSize1 != size1)
      throw new java.util.ConcurrentModificationException(
          "Map-size written was " + size1 + ", but element count was " + actualSize1 + ".");

  }

  @Override
  public void customDecode(org.apache.avro.io.ResolvingDecoder in) throws java.io.IOException {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      long size0 = in.readArrayStart();
      java.util.List<java.lang.String> a0 = this.arr;
      if (a0 == null) {
        a0 = new SpecificData.Array<java.lang.String>((int) size0, SCHEMA$.getField("arr").schema());
        this.arr = a0;
      } else
        a0.clear();
      SpecificData.Array<java.lang.String> ga0 = (a0 instanceof SpecificData.Array
          ? (SpecificData.Array<java.lang.String>) a0
          : null);
      for (; 0 < size0; size0 = in.arrayNext()) {
        for (; size0 != 0; size0--) {
          java.lang.String e0 = (ga0 != null ? ga0.peek() : null);
          e0 = in.readString();
          a0.add(e0);
        }
      }

      long size1 = in.readMapStart();
      java.util.Map<java.lang.String, java.lang.Long> m1 = this.map; // Need fresh name due to limitation of macro
                                                                     // system
      if (m1 == null) {
        m1 = new java.util.HashMap<java.lang.String, java.lang.Long>((int) size1);
        this.map = m1;
      } else
        m1.clear();
      for (; 0 < size1; size1 = in.mapNext()) {
        for (; size1 != 0; size1--) {
          java.lang.String k1 = null;
          k1 = in.readString();
          java.lang.Long v1 = null;
          v1 = in.readLong();
          m1.put(k1, v1);
        }
      }

    } else {
      for (int i = 0; i < 2; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          long size0 = in.readArrayStart();
          java.util.List<java.lang.String> a0 = this.arr;
          if (a0 == null) {
            a0 = new SpecificData.Array<java.lang.String>((int) size0, SCHEMA$.getField("arr").schema());
            this.arr = a0;
          } else
            a0.clear();
          SpecificData.Array<java.lang.String> ga0 = (a0 instanceof SpecificData.Array
              ? (SpecificData.Array<java.lang.String>) a0
              : null);
          for (; 0 < size0; size0 = in.arrayNext()) {
            for (; size0 != 0; size0--) {
              java.lang.String e0 = (ga0 != null ? ga0.peek() : null);
              e0 = in.readString();
              a0.add(e0);
            }
          }
          break;

        case 1:
          long size1 = in.readMapStart();
          java.util.Map<java.lang.String, java.lang.Long> m1 = this.map; // Need fresh name due to limitation of macro
                                                                         // system
          if (m1 == null) {
            m1 = new java.util.HashMap<java.lang.String, java.lang.Long>((int) size1);
            this.map = m1;
          } else
            m1.clear();
          for (; 0 < size1; size1 = in.mapNext()) {
            for (; size1 != 0; size1--) {
              java.lang.String k1 = null;
              k1 = in.readString();
              java.lang.Long v1 = null;
              v1 = in.readLong();
              m1.put(k1, v1);
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
