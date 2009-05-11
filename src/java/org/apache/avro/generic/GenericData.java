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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;

/** Utilities for generic Java data. */
public class GenericData {
  private GenericData() {}
  
  /** Default implementation of {@link GenericRecord}. */
  public static class Record
    extends HashMap<String,Object> implements GenericRecord {
    private final Schema schema;
    public Record(Schema schema) {
      super(schema.getFields().size());
      this.schema = schema;
    }
    public Schema getSchema() { return schema; }
  }

  /** Default implementation of {@link GenericArray}. */
  @SuppressWarnings(value="unchecked")
  public static class Array<T> implements GenericArray<T> {
    private static final Object[] EMPTY = new Object[0];
    private int size;
    private Object[] elements = EMPTY;
    public Array(int capacity) {
      if (capacity != 0)
        elements = new Object[capacity];
    }
    public long size() { return size; }
    public void clear() { size = 0; }
    public Iterator<T> iterator() {
      return new Iterator<T>() {
        private int position = 0;
        public boolean hasNext() { return position < size; }
        public T next() { return (T)elements[position++]; }
        public void remove() { throw new UnsupportedOperationException(); }
      };
    }
    public void add(T o) {
      if (size == elements.length) {
        Object[] newElements = new Object[(size * 3)/2 + 1];
        System.arraycopy(elements, 0, newElements, 0, size);
        elements = newElements;
      }
      elements[size++] = o;
    }
    public T peek() {
      return (size < elements.length) ? (T)elements[size] : null;
    }
    public int hashCode() {
      int hashCode = 1;
      for (T e : this)
        hashCode = 31*hashCode + (e==null ? 0 : e.hashCode());
      return hashCode;
    }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof GenericArray)) return false;
      Iterator e1 = iterator();
      Iterator e2 = ((GenericArray)o).iterator();
      while(e1.hasNext() && e2.hasNext()) {
        Object o1 = e1.next();
        Object o2 = e2.next();
        if (!(o1==null ? o2==null : o1.equals(o2)))
          return false;
      }
      return !(e1.hasNext() || e2.hasNext());
    }
  }

  /** Returns true if a Java datum matches a schema. */
  public static boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      if (!(datum instanceof GenericRecord)) return false;
      @SuppressWarnings(value="unchecked")
      GenericRecord fields = (GenericRecord)datum;
      for (Map.Entry<String, Schema> entry : schema.getFieldSchemas())
        if (!validate(entry.getValue(), fields.get(entry.getKey())))
          return false;
      return true;
    case ARRAY:
      if (!(datum instanceof GenericArray)) return false;
      for (Object element : (GenericArray)datum)
        if (!validate(schema.getElementType(), element))
          return false;
      return true;
    case MAP:
      if (!(datum instanceof Map)) return false;
      @SuppressWarnings(value="unchecked")
      Map<Object,Object> map = (Map<Object,Object>)datum;
      for (Map.Entry<Object,Object> entry : map.entrySet())
        if (!validate(schema.getValueType(), entry.getValue()))
          return false;
      return true;
    case UNION:
      for (Schema type : schema.getTypes())
        if (validate(type, datum))
          return true;
      return false;
    case STRING:  return datum instanceof Utf8;
    case BYTES:   return datum instanceof ByteBuffer;
    case INT:     return datum instanceof Integer;
    case LONG:    return datum instanceof Long;
    case FLOAT:   return datum instanceof Float;
    case DOUBLE:  return datum instanceof Double;
    case BOOLEAN: return datum instanceof Boolean;
    case NULL:    return datum == null;
    default: return false;
    }
  }

  /** Renders a Java datum as <a href="http://www.json.org/">JSON</a>. */
  public static String toString(Object datum) {
    StringBuilder buffer = new StringBuilder();
    toString(datum, buffer);
    return buffer.toString();
  }
  private static void toString(Object datum, StringBuilder buffer) {
    if (datum instanceof GenericRecord) {
      buffer.append("{");
      int count = 0;
      GenericRecord record = (GenericRecord)datum;
      for (Map.Entry<String,Object> entry : record.entrySet()) {
        toString(entry.getKey(), buffer);
        buffer.append(": ");
        toString(entry.getValue(), buffer);
        if (++count < record.size())
          buffer.append(", ");
      }
      buffer.append("}");
    } else if (datum instanceof GenericArray) {
      GenericArray array = (GenericArray)datum;
      buffer.append("[");
      long last = array.size()-1;
      int i = 0;
      for (Object element : array) {
        toString(element, buffer);
        if (i++ < last)
          buffer.append(", ");
      }        
      buffer.append("]");
    } else if (datum instanceof Map) {
      buffer.append("{");
      int count = 0;
      @SuppressWarnings(value="unchecked")
      Map<Object,Object> map = (Map<Object,Object>)datum;
      for (Map.Entry<Object,Object> entry : map.entrySet()) {
        toString(entry.getKey(), buffer);
        buffer.append(": ");
        toString(entry.getValue(), buffer);
        if (++count < map.size())
          buffer.append(", ");
      }
      buffer.append("}");
    } else if (datum instanceof Utf8 || datum instanceof String) {
      buffer.append("\"");
      buffer.append(datum);                       // TODO: properly escape!
      buffer.append("\"");
    } else if (datum instanceof ByteBuffer) {
      buffer.append("{\"bytes\": \"");
      ByteBuffer bytes = (ByteBuffer)datum;
      for (int i = bytes.position(); i < bytes.limit(); i++)
        buffer.append((char)bytes.get(i));
      buffer.append("\"}");
    } else {
      buffer.append(datum);
    }
  }

  /** Create a schema given an example datum. */
  public static Schema induce(Object datum) {
    if (datum instanceof GenericRecord) {
      @SuppressWarnings(value="unchecked")
      GenericRecord record = (GenericRecord)datum;
      LinkedHashMap<String,Field> fields = new LinkedHashMap<String,Field>();
      for (Map.Entry<String,Object> entry : record.entrySet())
        fields.put(entry.getKey(), new Field(induce(entry.getValue()), null));
      return Schema.createRecord(fields);
    } else if (datum instanceof GenericArray) {
      Schema elementType = null;
      for (Object element : (GenericArray)datum) {
        if (elementType == null) {
          elementType = induce(element);
        } else if (!elementType.equals(induce(element))) {
          throw new AvroTypeException("No mixed type arrays.");
        }
      }
      if (elementType == null) {
        throw new AvroTypeException("Empty array: "+datum);
      }
      return Schema.createArray(elementType);

    } else if (datum instanceof Map) {
      @SuppressWarnings(value="unchecked")
      Map<Object,Object> map = (Map<Object,Object>)datum;
      Schema value = null;
      for (Map.Entry<Object,Object> entry : map.entrySet()) {
        if (value == null) {
          value = induce(entry.getValue());
        } else if (!value.equals(induce(entry.getValue()))) {
          throw new AvroTypeException("No mixed type map values.");
        }
      }
      if (value == null) {
        throw new AvroTypeException("Empty map: "+datum);
      }
      return Schema.createMap(value);
    }
    else if (datum instanceof Utf8)       return Schema.create(Type.STRING);
    else if (datum instanceof ByteBuffer) return Schema.create(Type.BYTES);
    else if (datum instanceof Integer)    return Schema.create(Type.INT);
    else if (datum instanceof Long)       return Schema.create(Type.LONG);
    else if (datum instanceof Float)      return Schema.create(Type.FLOAT);
    else if (datum instanceof Double)     return Schema.create(Type.DOUBLE);
    else if (datum instanceof Boolean)    return Schema.create(Type.BOOLEAN);
    else if (datum == null)               return Schema.create(Type.NULL);

    else throw new AvroTypeException("Can't create schema for: "+datum);
  }
}
