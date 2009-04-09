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
import java.util.*;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

/** {@link DatumReader} for generic Java objects. */
public class GenericDatumReader implements DatumReader<Object> {
  private Schema actual;
  private Schema expected;

  public GenericDatumReader() {}

  public GenericDatumReader(Schema actual) {
    setSchema(actual);
  }

  public GenericDatumReader(Schema actual, Schema expected) {
    this(actual);
    this.expected = expected;
  }

  public void setSchema(Schema actual) { this.actual = actual; }

  public Object read(Object reuse, ValueReader in) throws IOException {
    return read(reuse, actual, expected != null ? expected : actual, in);
  }
  
  /** Called to read data.*/
  protected Object read(Object old, Schema actual,
                        Schema expected, ValueReader in) throws IOException {
    if (actual.getType() == Type.UNION)           // resolve unions
      actual = actual.getTypes().get((int)in.readLong());
    if (expected.getType() == Type.UNION)
      expected = resolveExpected(actual, expected);
    switch (actual.getType()) {
    case RECORD:  return readRecord(old, actual, expected, in);
    case ARRAY:   return readArray(old, actual, expected, in);
    case MAP:     return readMap(old, actual, expected, in);
    case STRING:  return readString(old, in);
    case BYTES:   return readBytes(old, in);
    case INT:     return in.readInt();
    case LONG:    return in.readLong();
    case FLOAT:   return in.readFloat();
    case DOUBLE:  return in.readDouble();
    case BOOLEAN: return in.readBoolean();
    case NULL:    return null;
    default: throw new AvroRuntimeException("Unknown type: "+actual);
    }
  }

  private Schema resolveExpected(Schema actual, Schema expected) {
    // first scan for exact match
    for (Schema branch : expected.getTypes())
      if (branch.getType() == actual.getType())
        if (branch.getType() == Type.RECORD) {
          String name = branch.getName();
          if (name == null || name.equals(actual.getName()))
            return branch;
        } else
          return branch;
    // then scan match via numeric promotion
    for (Schema branch : expected.getTypes())
      switch (actual.getType()) {
      case INT:
        switch (expected.getType()) {
        case LONG: case FLOAT: case DOUBLE:
          return expected;
        }
        break;
      case LONG:
        switch (expected.getType()) {
        case FLOAT: case DOUBLE:
          return expected;
        }
        break;
      case FLOAT:
        switch (expected.getType()) {
        case DOUBLE:
          return expected;
        }
        break;
      }
    throw new AvroTypeException("Expected "+expected+", found "+actual);
  }

  /** Called to read a record instance.  May be overridden for alternate record
   * representations.*/
  protected Object readRecord(Object old, Schema actual, Schema expected,
                              ValueReader in) throws IOException {
    String recordName = expected.getName();
    if (recordName != null && !recordName.equals(actual.getName()))
      throw new AvroTypeException("Expected "+expected+", found "+actual);
    Map<String,Schema> expectedFields = expected.getFields();
    GenericRecord record = newRecord(old, expected);
    int size = 0;
    for (Map.Entry<String,Schema> entry : actual.getFields().entrySet()) {
      String name = entry.getKey();
      Schema aField = entry.getValue();
      Schema eField = expected == actual ? aField : expectedFields.get(name);
      if (eField == null) {
        skip(aField, in);
        continue;
      }
      Object oldDatum = old != null ? record.get(name) : null;
      record.put(name, read(oldDatum, aField, eField, in));
      size++;
    }
    if (record.size() > size) {                   // clear old fields
      Iterator<String> i = record.keySet().iterator();
      while (i.hasNext()) {
        String f = i.next();
        if (!(actual.getFields().containsKey(f) &&
              expected.getFields().containsKey(f)))
          i.remove();
      }
    }
    return record;
  }

  /** Called to read a array instance.  May be overridden for alternate array
   * representations.*/
  @SuppressWarnings(value="unchecked")
  protected Object readArray(Object old, Schema actual, Schema expected,
                             ValueReader in) throws IOException {
    Schema actualType = actual.getElementType();
    Schema expectedType = expected.getElementType();
    long firstBlockSize = in.readLong();
    GenericArray array;
    if (old instanceof GenericArray) {
      array = (GenericArray)old;
      array.clear();
    } else
      array = newArray((int)firstBlockSize);
    for (long l = firstBlockSize; l > 0; l = in.readLong())
      for (long i = 0; i < l; i++)
        array.add(read(array.peek(), actualType, expectedType, in));
    return array;
  }

  /** Called to read a map instance.  May be overridden for alternate map
   * representations.*/
  @SuppressWarnings(value="unchecked")
  protected Object readMap(Object old, Schema actual, Schema expected,
                           ValueReader in) throws IOException {
    Schema aKey = actual.getKeyType();
    Schema aValue = actual.getValueType();
    Schema eKey = expected.getKeyType();
    Schema eValue = expected.getValueType();
    int firstBlockSize = (int)in.readLong();
    Map map;
    if (old instanceof Map) {
      map = (Map)old;
      map.clear();
    } else
      map = newMap(firstBlockSize);
    for (long l = firstBlockSize; l > 0; l = in.readLong())
      for (long i = 0; i < l; i++)
        map.put(read(null, aKey, eKey, in), read(null, aValue, eValue, in));
    return map;
  }

  /** Called to create new record instances.  Subclasses may override to use a
   * different record implementation.  By default, this returns a {@link
   * GenericData.Record}.*/
  protected GenericRecord newRecord(Object old, Schema schema) {
    if (old instanceof GenericRecord) {
      GenericRecord record = (GenericRecord)old;
      if (record.getSchema() == schema)
        return record;
    }
    return new GenericData.Record(schema);
  }

  /** Called to create new array instances.  Subclasses may override to use a
   * different array implementation.  By default, this returns a {@link
   * GenericData.Array}.*/
  protected GenericArray newArray(int size) {
    return new GenericData.Array(size);
  }

  /** Called to create new array instances.  Subclasses may override to use a
   * different map implementation.  By default, this returns a {@link
   * HashMap}.*/
  protected Map<Object,Object> newMap(int size) {
    return new HashMap<Object,Object>(size);
  }

  /** Called to read strings.  Subclasses may override to use a different
   * string representation.  By default, this calls {@link
   * ValueReader#readUtf8(Object)}.*/
  protected Object readString(Object old, ValueReader in) throws IOException {
    return in.readUtf8(old);
  }

  /** Called to read byte arrays.  Subclasses may override to use a different
   * byte array representation.  By default, this calls {@link
   * ValueReader#readBuffer(Object)}.*/
  protected Object readBytes(Object old, ValueReader in) throws IOException {
    return in.readBuffer(old);
  }

  /** Skip an instance of a schema. */
  public static void skip(Schema schema, ValueReader in) throws IOException {
    switch (schema.getType()) {
    case RECORD:
      for (Map.Entry<String,Schema> entry : schema.getFields().entrySet())
        skip(entry.getValue(), in);
      break;
    case ARRAY:
      Schema elementType = schema.getElementType();
      for (int l = (int)in.readLong(); l > 0; l = (int)in.readLong())
        for (int i = 0; i < l; i++)
          skip(elementType, in);
      break;
    case MAP:
      Schema key = schema.getKeyType();
      Schema value = schema.getValueType();
      for (int l = (int)in.readLong(); l > 0; l = (int)in.readLong())
        for (int i = 0; i < l; i++) {
          skip(key, in);
          skip(value, in);
        }
      break;
    case UNION:
      skip(schema.getTypes().get((int)in.readLong()), in);
      break;
    case STRING:
    case BYTES:
      long length = in.readLong();
      while (length > 0)
        length -= in.skip(length);
      break;
    case INT:     in.readInt();           break;
    case LONG:    in.readLong();          break;
    case FLOAT:   in.readFloat();         break;
    case DOUBLE:  in.readDouble();        break;
    case BOOLEAN: in.readBoolean();       break;
    case NULL:                            break;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

}
