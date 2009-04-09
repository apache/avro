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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

/** {@link DatumWriter} for generic Java objects. */
public class GenericDatumWriter implements DatumWriter<Object> {
  private Schema root;

  public GenericDatumWriter() {}

  public GenericDatumWriter(Schema root) {
    setSchema(root);
  }

  public void setSchema(Schema root) { this.root = root; }

  public void write(Object datum, ValueWriter out) throws IOException {
    write(root, datum, out);
  }
  
  /** Called to write data.*/
  protected void write(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    switch (schema.getType()) {
    case RECORD: writeRecord(schema, datum, out); break;
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
    if (!(datum instanceof GenericRecord)) error(schema,datum);
    GenericRecord record = (GenericRecord)datum;
    for (Map.Entry<String,Schema> entry : schema.getFields().entrySet())
      write(entry.getValue(), record.get(entry.getKey()), out);
    
  }

  /** Called to write a array.  May be overridden for alternate array
   * representations.*/
  protected void writeArray(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    if (!(datum instanceof GenericArray)) error(schema,datum);
    Schema element = schema.getElementType();
    GenericArray array = (GenericArray)datum;
    if (array.size() > 0) {
      out.writeLong(array.size());
      for (Object o : array)
        write(element, o, out);
    }
    out.writeLong(0);
  }

  /** Called to write a map.  May be overridden for alternate map
   * representations.*/
  protected void writeMap(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    if (!(datum instanceof Map)) error(schema,datum);
    Schema key = schema.getKeyType();
    Schema value = schema.getValueType();
    @SuppressWarnings(value="unchecked")
      Map<Object,Object> map = (Map<Object,Object>)datum;
    if (map.size() > 0) {
      out.writeLong(map.size());                // write a single block
      for (Map.Entry<Object,Object> entry : map.entrySet()) {
        write(key, entry.getKey(), out);
        write(value, entry.getValue(), out);
      }
    }
    out.writeLong(0);
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
      if (!(datum instanceof GenericRecord)) return false;
      return (schema.getName() == null) ||
        schema.getName().equals(((GenericRecord)datum).getSchema().getName());
    case ARRAY:
      return datum instanceof GenericArray;
    case MAP:
      return datum instanceof Map && !(datum instanceof GenericRecord);
    case STRING:  return datum instanceof Utf8;
    case BYTES:   return datum instanceof ByteBuffer;
    case INT:     return datum instanceof Integer;
    case LONG:    return datum instanceof Long;
    case FLOAT:   return datum instanceof Float;
    case DOUBLE:  return datum instanceof Double;
    case BOOLEAN: return datum instanceof Boolean;
    case NULL:    return datum == null;
    default: throw new AvroRuntimeException("Unexpected type: " +schema);
    }
  }

  private void error(Schema schema, Object datum) {
    throw new AvroTypeException("Not a "+schema+": "+datum);
  }

}

