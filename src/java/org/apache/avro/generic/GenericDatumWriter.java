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
import java.util.Set;
import java.util.Map.Entry;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.ValueWriter;
import org.apache.avro.util.Utf8;

/** {@link DatumWriter} for generic Java objects. */
public class GenericDatumWriter<D> implements DatumWriter<D> {
  private Schema root;

  public GenericDatumWriter() {}

  public GenericDatumWriter(Schema root) {
    setSchema(root);
  }

  public void setSchema(Schema root) { this.root = root; }

  public void write(D datum, ValueWriter out) throws IOException {
    write(root, datum, out);
  }
  
  /** Called to write data.*/
  protected void write(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    switch (schema.getType()) {
    case RECORD: writeRecord(schema, datum, out); break;
    case ENUM:   writeEnum(schema, datum, out);   break;
    case ARRAY:  writeArray(schema, datum, out);  break;
    case MAP:    writeMap(schema, datum, out);    break;
    case UNION:
      int index = resolveUnion(schema, datum);
      out.writeLong(index);
      write(schema.getTypes().get(index), datum, out);
      break;
    case STRING:  writeString(datum, out);          break;
    case BYTES:   writeBytes(datum, out);           break;
    case INT:     out.writeInt((Integer)datum);     break;
    case LONG:    out.writeLong((Long)datum);       break;
    case FLOAT:   out.writeFloat((Float)datum);     break;
    case DOUBLE:  out.writeDouble((Double)datum);   break;
    case BOOLEAN: out.writeBoolean((Boolean)datum); break;
    case NULL:                                      break;
    default: error(schema,datum);
    }
  }

  /** Called to write a record.  May be overridden for alternate record
   * representations.*/
  protected void writeRecord(Schema schema, Object datum, ValueWriter out)
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
  protected void writeEnum(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    out.writeInt(schema.getEnumOrdinal((String)datum));
  }
  
  /** Called to write a array.  May be overridden for alternate array
   * representations.*/
  protected void writeArray(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    Schema element = schema.getElementType();
    long size = getArraySize(datum);
    if (size > 0) {
      out.writeLong(size);
      for (Iterator<? extends Object> it = getArrayElements(datum); it.hasNext();)
        write(element, it.next(), out);
    }
    out.writeLong(0);
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
  protected void writeMap(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    Schema value = schema.getValueType();
    int size = getMapSize(datum);
    if (size > 0) {
      out.writeLong(size);                // write a single block
      for (Map.Entry<Object,Object> entry : getMapEntries(datum)) {
        writeString(entry.getKey(), out);
        write(value, entry.getValue(), out);
      }
    }
    out.writeLong(0);
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
  protected void writeString(Object datum, ValueWriter out) throws IOException {
    out.writeUtf8((Utf8)datum);
  }

  /** Called to write a bytes.  May be overridden for alternate bytes
   * representations.*/
  protected void writeBytes(Object datum, ValueWriter out) throws IOException {
    out.writeBuffer((ByteBuffer)datum);
  }

  private int resolveUnion(Schema union, Object datum) {
    int i = 0;
    for (Schema type : union.getTypes()) {
      if (instanceOf(type, datum))
        return i;
      i++;
    }
    throw new AvroRuntimeException("Not in union "+union+": "+datum);
  }

  /** Called to resolve unions.  May be overridden for alternate data
      representations.*/
  protected boolean instanceOf(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      if (!isRecord(datum)) return false;
      return (schema.getName() == null) ||
        schema.getName().equals(((GenericRecord)datum).getSchema().getName());
    case ENUM:    return isEnum(datum);
    case ARRAY:   return isArray(datum);
    case MAP:     return isMap(datum);
    case STRING:  return isString(datum);
    case BYTES:   return isBytes(datum);
    case INT:     return datum instanceof Integer;
    case LONG:    return datum instanceof Long;
    case FLOAT:   return datum instanceof Float;
    case DOUBLE:  return datum instanceof Double;
    case BOOLEAN: return datum instanceof Boolean;
    case NULL:    return datum == null;
    default: throw new AvroRuntimeException("Unexpected type: " +schema);
    }
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isArray(Object datum) {
    return datum instanceof GenericArray;
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isRecord(Object datum) {
    return datum instanceof GenericRecord;
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isEnum(Object datum) {
    return datum instanceof String;
  }
  
  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isMap(Object datum) {
    return (datum instanceof Map) && (!(datum instanceof GenericRecord));
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isString(Object datum) {
    return datum instanceof Utf8;
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isBytes(Object datum) {
    return datum instanceof ByteBuffer;
  }
  
  private void error(Schema schema, Object datum) {
    throw new AvroTypeException("Not a "+schema+": "+datum);
  }

}

