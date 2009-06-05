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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.nio.ByteBuffer;

import org.codehaus.jackson.JsonNode;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.ValueReader;
import org.apache.avro.util.Utf8;

/** {@link DatumReader} for generic Java objects. */
public class GenericDatumReader<D> implements DatumReader<D> {
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

  @SuppressWarnings("unchecked")
  public D read(D reuse, ValueReader in) throws IOException {
    return (D) read(reuse, actual, expected != null ? expected : actual, in);
  }
  
  /** Called to read data.*/
  protected Object read(Object old, Schema actual,
                        Schema expected, ValueReader in) throws IOException {
    if (actual.getType() == Type.UNION)           // resolve unions
      actual = actual.getTypes().get((int)in.readIndex());
    if (expected.getType() == Type.UNION)
      expected = resolveExpected(actual, expected);
    switch (actual.getType()) {
    case RECORD:  return readRecord(old, actual, expected, in);
    case ENUM:    return readEnum(actual, expected, in);
    case ARRAY:   return readArray(old, actual, expected, in);
    case MAP:     return readMap(old, actual, expected, in);
    case FIXED:   return readFixed(old, actual, expected, in);
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
        switch (branch.getType()) {
        case RECORD:
        case FIXED:
          String name = branch.getName();
          if (name == null || name.equals(actual.getName()))
            return branch;
          break;
        default:
          return branch;
        }
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

  /** Called to read a record instance. May be overridden for alternate record
   * representations.*/
  protected Object readRecord(Object old, Schema actual, Schema expected,
                              ValueReader in) throws IOException {
    /* TODO: We may want to compute the expected and actual mapping and cache
     * the mapping (keyed by <actual, expected>). */
    String recordName = expected.getName();
    if (recordName != null && !recordName.equals(actual.getName()))
      throw new AvroTypeException("Expected "+expected+", found "+actual);
    Map<String, Field> expectedFields = expected.getFields();
    // all fields not in expected should be removed by newRecord.
    Object record = newRecord(old, expected);
    int size = 0;
    for (Map.Entry<String, Field> entry : actual.getFields().entrySet()) {
      String fieldName = entry.getKey();
      Field actualField = entry.getValue();
      Field expectedField =
          expected == actual ? actualField : expectedFields.get(entry.getKey());
      if (expectedField == null) {
        skip(actualField.schema(), in);
        continue;
      }
      int fieldPosition = expectedField.pos();
      Object oldDatum =
          (old != null) ? getField(record, fieldName, fieldPosition) : null;
      addField(record, fieldName, fieldPosition,
               read(oldDatum,actualField.schema(),expectedField.schema(), in));
      size++;
    }
    if (expectedFields.size() > size) {           // not all fields set
      Set<String> actualFields = actual.getFields().keySet();
      for (Map.Entry<String, Field> entry : expectedFields.entrySet()) {
        String fieldName = entry.getKey();
        if (!actualFields.contains(fieldName)) {  // an unset field
          Field f = entry.getValue();
          JsonNode json = f.defaultValue();
          if (json != null)                       // has default
            addField(record, fieldName, f.pos(),  // add default
                     defaultFieldValue(old, f.schema(), json));
          else if (old != null)                   // remove stale value
            removeField(record, fieldName, entry.getValue().pos());
        }
      }
    }
    return record;
  }

  /** Called by the default implementation of {@link #readRecord} to add a
   * record fields value to a record instance.  The default implementation is
   * for {@link GenericRecord}.*/
  protected void addField(Object record, String name, int position, Object o) {
    ((GenericRecord) record).put(name, o);
  }
  
  /** Called by the default implementation of {@link #readRecord} to retrieve a
   * record field value from a reused instance.  The default implementation is
   * for {@link GenericRecord}.*/
  protected Object getField(Object record, String name, int position) {
    return ((GenericRecord) record).get(name);
  }

  /** Called by the default implementation of {@link #readRecord} to remove a
   * record field value from a reused instance.  The default implementation is
   * for {@link GenericRecord}.*/
  protected void removeField(Object record, String field, int position) {
    ((GenericRecord) record).remove(field);
  }
  
  /** Called by the default implementation of {@link #readRecord} to construct
      a default value for a field. */
  protected Object defaultFieldValue(Object old, Schema schema, JsonNode json) {
    switch (schema.getType()) {
    case RECORD:
      Object record = newRecord(old, schema);
      for (Map.Entry<String, Field> entry : schema.getFields().entrySet()) {
        String name = entry.getKey();
        Field f = entry.getValue();
        JsonNode v = json.get(name);
        if (v == null) v = f.defaultValue();
        if (v != null) {
          Object o = old != null ? getField(old, name, f.pos()) : null;
          addField(record, name, f.pos(), defaultFieldValue(o, f.schema(), v));
        } else if (old != null) {
          removeField(record, name, f.pos());
        }
      }
      return record;
    case ENUM:
      return createEnum(json.getTextValue(), schema);
    case ARRAY:
      Object array = newArray(old, json.size());
      Schema element = schema.getElementType();
      for (JsonNode node : json)
        addToArray(array, defaultFieldValue(peekArray(array), element, node));
      return array;
    case MAP:
      Object map = newMap(old, json.size());
      Schema value = schema.getValueType();
      for (Iterator<String> i = json.getFieldNames(); i.hasNext();) {
        String key = i.next();
        addToMap(map, new Utf8(key),
                 defaultFieldValue(null, value, json.get(key)));
      }
      return map;
    case UNION:   return defaultFieldValue(old, schema.getTypes().get(0), json);
    case FIXED:   return createFixed(old,json.getTextValue().getBytes(),schema);
    case STRING:  return createString(json.getTextValue());
    case BYTES:   return createBytes(json.getTextValue().getBytes());
    case INT:     return json.getIntValue();
    case LONG:    return json.getLongValue();
    case FLOAT:   return (float)json.getDoubleValue();
    case DOUBLE:  return json.getDoubleValue();
    case BOOLEAN: return json.getBooleanValue();
    case NULL:    return null;
    default: throw new AvroRuntimeException("Unknown type: "+actual);
    }
  }

  /** Called to read an enum value. May be overridden for alternate enum
   * representations.  By default, returns the symbol as a String. */
  protected Object readEnum(Schema actual, Schema expected, ValueReader in)
    throws IOException {
    String name = expected.getName();
    if (name != null && !name.equals(actual.getName()))
      throw new AvroTypeException("Expected "+expected+", found "+actual);
    return createEnum(actual.getEnumSymbols().get(in.readEnum()), expected);
  }

  /** Called to create an enum value. May be overridden for alternate enum
   * representations.  By default, returns the symbol as a String. */
  protected Object createEnum(String symbol, Schema schema) { return symbol; }

  /** Called to read an array instance.  May be overridden for alternate array
   * representations.*/
  protected Object readArray(Object old, Schema actual, Schema expected,
                             ValueReader in) throws IOException {
    Schema actualType = actual.getElementType();
    Schema expectedType = expected.getElementType();
    long l = in.readArrayStart();
    if (l > 0) {
      Object array = newArray(old, (int) l);
      do {
        for (long i = 0; i < l; i++) {
          addToArray(array, read(peekArray(array), actualType, expectedType, in));  
        }
      } while ((l = in.arrayNext()) > 0);
      
      return array;
    } else {
      return newArray(old, 0);
    }
  }

  /** Called by the default implementation of {@link #readArray} to retrieve a
   * value from a reused instance.  The default implementation is for {@link
   * GenericArray}.*/
  @SuppressWarnings("unchecked")
  protected Object peekArray(Object array) {
    return ((GenericArray) array).peek();
  }

  /** Called by the default implementation of {@link #readArray} to add a value.
   * The default implementation is for {@link GenericArray}.*/
  @SuppressWarnings("unchecked")
  protected void addToArray(Object array, Object e) {
    ((GenericArray) array).add(e);
  }
  
  /** Called to read a map instance.  May be overridden for alternate map
   * representations.*/
  protected Object readMap(Object old, Schema actual, Schema expected,
                           ValueReader in) throws IOException {
    Schema aValue = actual.getValueType();
    Schema eValue = expected.getValueType();
    long l = in.readMapStart();
    Object map = newMap(old, (int) l);
    if (l > 0) {
      do {
        for (int i = 0; i < l; i++) {
          addToMap(map,
              readString(null, in),
              read(null, aValue, eValue, in));
        }
      } while ((l = in.mapNext()) > 0);
    }
    return map;
  }

  /** Called by the default implementation of {@link #readMap} to add a
   * key/value pair.  The default implementation is for {@link Map}.*/
  @SuppressWarnings("unchecked")
  protected void addToMap(Object map, Object key, Object value) {
    ((Map) map).put(key, value);
  }
  
  /** Called to read a fixed value. May be overridden for alternate fixed
   * representations.  By default, returns {@link GenericFixed}. */
  protected Object readFixed(Object old, Schema actual, Schema expected,
                             ValueReader in)
    throws IOException {
    if (!actual.equals(expected))
      throw new AvroTypeException("Expected "+expected+", found "+actual);
    GenericFixed fixed = (GenericFixed)createFixed(old, expected);
    in.readFixed(fixed.bytes(), 0, actual.getFixedSize());
    return fixed;
  }

  /** Called to create an fixed value. May be overridden for alternate fixed
   * representations.  By default, returns {@link GenericFixed}. */
  protected Object createFixed(Object old, Schema schema) {
    if ((old instanceof GenericFixed)
        && ((GenericFixed)old).bytes().length == schema.getFixedSize())
      return old;
    return new GenericData.Fixed(schema);
  }

  /** Called to create an fixed value. May be overridden for alternate fixed
   * representations.  By default, returns {@link GenericFixed}. */
  protected Object createFixed(Object old, byte[] bytes, Schema schema) {
    GenericFixed fixed = (GenericFixed)createFixed(old, schema);
    System.arraycopy(bytes, 0, fixed.bytes(), 0, schema.getFixedSize());
    return fixed;
  }
  /**
   * Called to create new record instances. Subclasses may override to use a
   * different record implementation. The returned instance must conform to the
   * schema provided. If the old object contains fields not present in the
   * schema, they should either be removed from the old object, or it should
   * create a new instance that conforms to the schema. By default, this returns
   * a {@link GenericData.Record}.
   */
  protected Object newRecord(Object old, Schema schema) {
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
  @SuppressWarnings("unchecked")
  protected Object newArray(Object old, int size) {
    if (old instanceof GenericArray) {
      ((GenericArray) old).clear();
      return old;
    } else return new GenericData.Array(size);
  }

  /** Called to create new array instances.  Subclasses may override to use a
   * different map implementation.  By default, this returns a {@link
   * HashMap}.*/
  @SuppressWarnings("unchecked")
  protected Object newMap(Object old, int size) {
    if (old instanceof Map) {
      ((Map) old).clear();
      return old;
    } else return new HashMap<Object, Object>(size);
  }

  /** Called to read strings.  Subclasses may override to use a different
   * string representation.  By default, this calls {@link
   * ValueReader#readString(Utf8)}.*/
  protected Object readString(Object old, ValueReader in) throws IOException {
    return in.readString((Utf8)old);
  }

  /** Called to create a string from a default value.  Subclasses may override
   * to use a different string representation.  By default, this calls {@link
   * Utf8#Utf8(String)}.*/
  protected Object createString(String value) { return new Utf8(value); }

  /** Called to read byte arrays.  Subclasses may override to use a different
   * byte array representation.  By default, this calls {@link
   * ValueReader#readBytes(ByteBuffer)}.*/
  protected Object readBytes(Object old, ValueReader in) throws IOException {
    return in.readBytes((ByteBuffer)old);
  }

  /** Called to create byte arrays from default values.  Subclasses may
   * override to use a different byte array representation.  By default, this
   * calls {@link ByteBuffer#wrap(byte[])}.*/
  protected Object createBytes(byte[] value) { return ByteBuffer.wrap(value); }

  private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);

  /** Skip an instance of a schema. */
  public static void skip(Schema schema, ValueReader in) throws IOException {
    switch (schema.getType()) {
    case RECORD:
      for (Map.Entry<String, Schema> entry : schema.getFieldSchemas())
        skip(entry.getValue(), in);
      break;
    case ENUM:
      in.readInt();
      break;
    case ARRAY:
      Schema elementType = schema.getElementType();
      for (long l = in.skipArray(); l > 0; l = in.skipArray()) {
        for (long i = 0; i < l; i++) {
          skip(elementType, in);
        }
      }
      break;
    case MAP:
      Schema value = schema.getValueType();
      for (long l = in.skipMap(); l > 0; l = in.skipMap()) {
        for (long i = 0; i < l; i++) {
          skip(STRING_SCHEMA, in);
          skip(value, in);
        }
      }
      break;
    case UNION:
      skip(schema.getTypes().get((int)in.readIndex()), in);
      break;
    case FIXED:
      in.skipFixed(schema.getFixedSize());
      break;
    case STRING:
    case BYTES:
      in.skipBytes();
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
