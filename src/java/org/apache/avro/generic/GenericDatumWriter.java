/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.generic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

/** {@link DatumWriter} for generic Java objects. */
public class GenericDatumWriter<D> implements DatumWriter<D> {
  private final GenericData data;
  private Schema root;

  public GenericDatumWriter() { this(GenericData.get()); }

  protected GenericDatumWriter(GenericData data) { this.data = data; }

  public GenericDatumWriter(Schema root) {
    this();
    setSchema(root);
  }

  protected GenericDatumWriter(Schema root, GenericData data) {
    this(data);
    setSchema(root);
  }

  public void setSchema(Schema root) { this.root = root; }

  public void write(D datum, Encoder out) throws IOException {
    write(root, datum, out);
  }
  
  /** Called to write data.*/
  protected void write(Schema schema, Object datum, Encoder out)
    throws IOException {
    switch (schema.getType()) {
    case RECORD: writeRecord(schema, datum, out); break;
    case ENUM:   writeEnum(schema, datum, out);   break;
    case ARRAY:  writeArray(schema, datum, out);  break;
    case MAP:    writeMap(schema, datum, out);    break;
    case UNION:
      int index = data.resolveUnion(schema, datum);
      out.writeIndex(index);
      write(schema.getTypes().get(index), datum, out);
      break;
    case FIXED:   writeFixed(schema, datum, out);   break;
    case STRING:  writeString(datum, out);          break;
    case BYTES:   writeBytes(datum, out);           break;
    case INT:     out.writeInt((Integer)datum);     break;
    case LONG:    out.writeLong((Long)datum);       break;
    case FLOAT:   out.writeFloat((Float)datum);     break;
    case DOUBLE:  out.writeDouble((Double)datum);   break;
    case BOOLEAN: out.writeBoolean((Boolean)datum); break;
    case NULL:    out.writeNull();                  break;
    default: error(schema,datum);
    }
  }

  /** Called to write a record.  May be overridden for alternate record
   * representations.*/
  protected void writeRecord(Schema schema, Object datum, Encoder out)
    throws IOException {
    for (Entry<String, Field> entry : schema.getFields().entrySet()) {
      Field field = entry.getValue();
      write(field.schema(), getField(datum, entry.getKey(), field.pos()), out);
    }
  }
  
  /** Called by the default implementation of {@link #writeRecord} to retrieve
   * a record field value.  The default implementation is for {@link
   * GenericRecord}.*/
  protected Object getField(Object record, String field, int position) {
    return ((GenericRecord) record).get(field);
  }
  
  /** Called to write an enum value.  May be overridden for alternate enum
   * representations.*/
  protected void writeEnum(Schema schema, Object datum, Encoder out)
    throws IOException {
    out.writeEnum(schema.getEnumOrdinal((String)datum));
  }
  
  /** Called to write a array.  May be overridden for alternate array
   * representations.*/
  protected void writeArray(Schema schema, Object datum, Encoder out)
    throws IOException {
    Schema element = schema.getElementType();
    long size = getArraySize(datum);
    out.writeArrayStart();
    out.setItemCount(size);
    for (Iterator<? extends Object> it = getArrayElements(datum); it.hasNext();) {
      out.startItem();
      write(element, it.next(), out);
    }
    out.writeArrayEnd();
  }

  /** Called by the default implementation of {@link #writeArray} to get the
   * size of an array.  The default implementation is for {@link
   * GenericArray}.*/
  @SuppressWarnings("unchecked")
  protected long getArraySize(Object array) {
    return ((GenericArray) array).size();
  }

  /** Called by the default implementation of {@link #writeArray} to enumerate
   * array elements.  The default implementation is for {@link GenericArray}.*/
  @SuppressWarnings("unchecked")
  protected Iterator<? extends Object> getArrayElements(Object array) {
    return ((GenericArray) array).iterator();
  }
  
  /** Called to write a map.  May be overridden for alternate map
   * representations.*/
  protected void writeMap(Schema schema, Object datum, Encoder out)
    throws IOException {
    Schema value = schema.getValueType();
    int size = getMapSize(datum);
    out.writeMapStart();
    out.setItemCount(size);
    for (Map.Entry<Object,Object> entry : getMapEntries(datum)) {
      out.startItem();
      out.writeString((Utf8) entry.getKey());
      write(value, entry.getValue(), out);
    }
    out.writeMapEnd();
  }

  /** Called by the default implementation of {@link #writeMap} to get the size
   * of a map.  The default implementation is for {@link Map}.*/
  @SuppressWarnings("unchecked")
  protected int getMapSize(Object map) {
    return ((Map) map).size();
  }

  /** Called by the default implementation of {@link #writeMap} to enumerate
   * map elements.  The default implementation is for {@link Map}.*/
  @SuppressWarnings("unchecked")
  protected Iterable<Map.Entry<Object,Object>> getMapEntries(Object map) {
    return ((Map) map).entrySet();
  }
  
  /** Called to write a string.  May be overridden for alternate string
   * representations.*/
  protected void writeString(Object datum, Encoder out) throws IOException {
    out.writeString((Utf8)datum);
  }

  /** Called to write a bytes.  May be overridden for alternate bytes
   * representations.*/
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    out.writeBytes((ByteBuffer)datum);
  }

  /** Called to write a fixed value.  May be overridden for alternate fixed
   * representations.*/
  protected void writeFixed(Schema schema, Object datum, Encoder out)
    throws IOException {
    out.writeFixed(((GenericFixed)datum).bytes(), 0, schema.getFixedSize());
  }
  
  private void error(Schema schema, Object datum) {
    throw new AvroTypeException("Not a "+schema+": "+datum);
  }

}

