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
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.io.BinaryData;
import org.apache.avro.util.Utf8;

/** Utilities for generic Java data. */
public class GenericData {

  private static final GenericData INSTANCE = new GenericData();
  
  /** Return the singleton instance. */
  public static GenericData get() { return INSTANCE; }

  protected GenericData() {}
  
  /** Default implementation of {@link GenericRecord}. */
  public static class Record implements GenericRecord, Comparable<Record> {
    private final Schema schema;
    private final Object[] values;
    public Record(Schema schema) {
      if (schema == null || !Type.RECORD.equals(schema.getType()))
        throw new AvroRuntimeException("Not a record schema: "+schema);
      this.schema = schema;
      this.values = new Object[schema.getFields().size()];
    }
    @Override public Schema getSchema() { return schema; }
    @Override public void put(String key, Object value) {
      values[schema.getField(key).pos()] = value;
    }
    @Override public void put(int i, Object v) { values[i] = v; }
    @Override public Object get(String key) {
      Field field = schema.getField(key);
      if (field == null) return null;
      return values[field.pos()];
    }
    @Override public Object get(int i) { return values[i]; }
    @Override public boolean equals(Object o) {
      if (o == this) return true;                 // identical object
      if (!(o instanceof Record)) return false;   // not a record
      Record that = (Record)o;
      if (!schema.getFullName().equals(that.schema.getFullName()))
        return false;                             // not the same schema
      return this.compareTo(that) == 0;
    }
    @Override public int hashCode() {
      return GenericData.get().hashCode(this, schema);
    }
    @Override public int compareTo(Record that) {
      return GenericData.get().compare(this, that, schema);
    }
    @Override public String toString() {
      return GenericData.get().toString(this);
    }
  }

  /** Default implementation of an array. */
  @SuppressWarnings(value="unchecked")
  public static class Array<T> extends AbstractList<T>
    implements GenericArray<T>, Comparable<GenericArray<T>> {
    private static final Object[] EMPTY = new Object[0];
    private final Schema schema;
    private int size;
    private Object[] elements = EMPTY;
    public Array(int capacity, Schema schema) {
      if (schema == null || !Type.ARRAY.equals(schema.getType()))
        throw new AvroRuntimeException("Not an array schema: "+schema);
      this.schema = schema;
      if (capacity != 0)
        elements = new Object[capacity];
    }
    public Schema getSchema() { return schema; }
    @Override public int size() { return size; }
    @Override public void clear() { size = 0; }
    @Override public Iterator<T> iterator() {
      return new Iterator<T>() {
        private int position = 0;
        public boolean hasNext() { return position < size; }
        public T next() { return (T)elements[position++]; }
        public void remove() { throw new UnsupportedOperationException(); }
      };
    }
    @Override public T get(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      return (T)elements[i];
    }
    @Override public boolean add(T o) {
      if (size == elements.length) {
        Object[] newElements = new Object[(size * 3)/2 + 1];
        System.arraycopy(elements, 0, newElements, 0, size);
        elements = newElements;
      }
      elements[size++] = o;
      return true;
    }
    @Override public T set(int i, T o) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      T response = (T)elements[i];
      elements[i] = o;
      return response;
    }
    @Override public T remove(int i) {
      if (i >= size)
        throw new IndexOutOfBoundsException("Index " + i + " out of bounds.");
      T result = (T)elements[i];
      --size;
      System.arraycopy(elements, i+1, elements, i, (size-i));
      elements[size] = null;
      return result;
    }
    public T peek() {
      return (size < elements.length) ? (T)elements[size] : null;
    }
    @Override
    public int hashCode() {
      return GenericData.get().hashCode(this, schema);
    }
    @Override
    public boolean equals(Object o) {
      if (o == this) return true;                 // identical object
      if (!(o instanceof Array)) return false;    // not an array
      Array that = (Array)o;
      if (!schema.equals(that.schema))
        return false;                             // not the same schema
      return this.compareTo(that) == 0;
    }
    public int compareTo(GenericArray<T> that) {
      return GenericData.get().compare(this, that, this.getSchema());
    }
    public void reverse() {
      int left = 0;
      int right = elements.length - 1;
      
      while (left < right) {
        Object tmp = elements[left];
        elements[left] = elements[right];
        elements[right] = tmp;
        
        left++;
        right--;
      }
    }
    @Override
    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append("[");
      int count = 0;
      for (T e : this) {
        buffer.append(e==null ? "null" : e.toString());
        if (++count < size())
          buffer.append(", ");
      }
      buffer.append("]");
      return buffer.toString();
    }
  }

  /** Default implementation of {@link GenericFixed}. */
  public static class Fixed implements GenericFixed, Comparable<Fixed> {
    private byte[] bytes;

    public Fixed(Schema schema) { bytes(new byte[schema.getFixedSize()]); }
    public Fixed(byte[] bytes) { bytes(bytes); }

    protected Fixed() {}
    public void bytes(byte[] bytes) { this.bytes = bytes; }

    public byte[] bytes() { return bytes; }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      return o instanceof GenericFixed
        && Arrays.equals(bytes, ((GenericFixed)o).bytes());
    }

    @Override
    public int hashCode() { return Arrays.hashCode(bytes); }

    @Override
    public String toString() { return Arrays.toString(bytes); }

    public int compareTo(Fixed that) {
      return BinaryData.compareBytes(this.bytes, 0, this.bytes.length,
                                     that.bytes, 0, that.bytes.length);
    }
  }

  /** Default implementation of {@link GenericEnumSymbol}. */
  public static class EnumSymbol implements GenericEnumSymbol {
    private String symbol;
    public EnumSymbol(String symbol) { this.symbol = symbol; }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      return o instanceof GenericEnumSymbol
        && symbol.equals(o.toString());
    }

    @Override
    public int hashCode() { return symbol.hashCode(); }

    @Override
    public String toString() { return symbol; }
  }

  /** Returns true if a Java datum matches a schema. */
  public boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      if (!(datum instanceof IndexedRecord)) return false;
      IndexedRecord fields = (IndexedRecord)datum;
      for (Field f : schema.getFields()) {
        if (!validate(f.schema(), fields.get(f.pos())))
          return false;
      }
      return true;
    case ENUM:
      return schema.getEnumSymbols().contains(datum.toString());
    case ARRAY:
      if (!(datum instanceof Collection)) return false;
      for (Object element : (Collection<?>)datum)
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
    case FIXED:
      return datum instanceof GenericFixed
        && ((GenericFixed)datum).bytes().length==schema.getFixedSize();
    case STRING:  return isString(datum);
    case BYTES:   return isBytes(datum);
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
  public String toString(Object datum) {
    StringBuilder buffer = new StringBuilder();
    toString(datum, buffer);
    return buffer.toString();
  }
  /** Renders a Java datum as <a href="http://www.json.org/">JSON</a>. */
  protected void toString(Object datum, StringBuilder buffer) {
    if (datum instanceof IndexedRecord) {
      buffer.append("{");
      int count = 0;
      IndexedRecord record = (IndexedRecord)datum;
      for (Field f : record.getSchema().getFields()) {
        toString(f.name(), buffer);
        buffer.append(": ");
        toString(record.get(f.pos()), buffer);
        if (++count < record.getSchema().getFields().size())
          buffer.append(", ");
      }
      buffer.append("}");
    } else if (datum instanceof Collection) {
      Collection<?> array = (Collection<?>)datum;
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
    } else if (datum instanceof CharSequence
               || datum instanceof GenericEnumSymbol) {
      buffer.append("\"");
      writeEscapedString(datum.toString(), buffer);
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
  
  /* Adapted from http://code.google.com/p/json-simple */
  private void writeEscapedString(String string, StringBuilder builder) {
    for(int i = 0; i < string.length(); i++){
      char ch = string.charAt(i);
      switch(ch){
        case '"':
          builder.append("\\\"");
          break;
        case '\\':
          builder.append("\\\\");
          break;
        case '\b':
          builder.append("\\b");
          break;
        case '\f':
          builder.append("\\f");
          break;
        case '\n':
          builder.append("\\n");
          break;
        case '\r':
          builder.append("\\r");
          break;
        case '\t':
          builder.append("\\t");
          break;
        case '/':
          builder.append("\\/");
          break;
        default:
          // Reference: http://www.unicode.org/versions/Unicode5.1.0/
          if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
            String hex = Integer.toHexString(ch);
            builder.append("\\u");
            for(int j = 0; j < 4-builder.length(); j++)
              builder.append('0');
            builder.append(string.toUpperCase());
          } else {
            builder.append(ch);
          }
        }
    }
  }

  /** Create a schema given an example datum. */
  public Schema induce(Object datum) {
    if (datum instanceof IndexedRecord) {
      return ((IndexedRecord)datum).getSchema();
    } else if (datum instanceof Collection) {
      Schema elementType = null;
      for (Object element : (Collection<?>)datum) {
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
    } else if (datum instanceof GenericFixed) {
      return Schema.createFixed(null, null, null,
                                ((GenericFixed)datum).bytes().length);
    }
    else if (datum instanceof CharSequence) return Schema.create(Type.STRING);
    else if (datum instanceof ByteBuffer) return Schema.create(Type.BYTES);
    else if (datum instanceof Integer)    return Schema.create(Type.INT);
    else if (datum instanceof Long)       return Schema.create(Type.LONG);
    else if (datum instanceof Float)      return Schema.create(Type.FLOAT);
    else if (datum instanceof Double)     return Schema.create(Type.DOUBLE);
    else if (datum instanceof Boolean)    return Schema.create(Type.BOOLEAN);
    else if (datum == null)               return Schema.create(Type.NULL);

    else throw new AvroTypeException("Can't create schema for: "+datum);
  }

  /** Called by {@link GenericDatumReader#readRecord} to set a record fields
   * value to a record instance.  The default implementation is for {@link
   * IndexedRecord}.*/
  public void setField(Object record, String name, int position, Object o) {
    ((IndexedRecord)record).put(position, o);
  }
  
  /** Called by {@link GenericDatumReader#readRecord} to retrieve a record
   * field value from a reused instance.  The default implementation is for
   * {@link IndexedRecord}.*/
  public Object getField(Object record, String name, int position) {
    return ((IndexedRecord)record).get(position);
  }

  /** Return the index for a datum within a union.  Implemented with {@link
   * #instanceOf(Schema,Object)}.*/
  public int resolveUnion(Schema union, Object datum) {
    int i = 0;
    for (Schema type : union.getTypes()) {
      if (instanceOf(type, datum))
        return i;
      i++;
    }
    throw new UnresolvedUnionException(union, datum);
  }

  /** Called by {@link #resolveUnion(Schema,Object)}.  May be overridden for
      alternate data representations.*/
  protected boolean instanceOf(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      if (!isRecord(datum)) return false;
      return (schema.getName() == null) ||
        schema.getName().equals(getRecordSchema(datum).getName());
    case ENUM:    return isEnum(datum);
    case ARRAY:   return isArray(datum);
    case MAP:     return isMap(datum);
    case FIXED:   return isFixed(datum);
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
    return datum instanceof Collection;
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isRecord(Object datum) {
    return datum instanceof IndexedRecord;
  }

  /** Called to obtain the schema of a record.  By default calls
   * {GenericContainer#getSchema().  May be overridden for alternate record
   * representations. */
  protected Schema getRecordSchema(Object record) {
    return ((GenericContainer)record).getSchema();
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isEnum(Object datum) {
    return datum instanceof GenericEnumSymbol;
  }
  
  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isMap(Object datum) {
    return datum instanceof Map;
  }
  
  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isFixed(Object datum) {
    return datum instanceof GenericFixed;
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isString(Object datum) {
    return datum instanceof CharSequence;
  }

  /** Called by the default implementation of {@link #instanceOf}.*/
  protected boolean isBytes(Object datum) {
    return datum instanceof ByteBuffer;
  }

  /** Compute a hash code according to a schema, consistent with {@link
   * #compare(Object,Object,Schema)}. */
  public int hashCode(Object o, Schema s) {
    if (o == null) return 0;                      // incomplete datum
    int hashCode = 1;
    switch (s.getType()) {
    case RECORD:
      IndexedRecord r = (IndexedRecord)o;
      for (Field f : s.getFields()) {
        if (f.order() == Field.Order.IGNORE)
          continue;
        hashCode = hashCodeAdd(hashCode, r.get(f.pos()), f.schema());
      }
      return hashCode;
    case ARRAY:
      Collection<?> a = (Collection<?>)o;
      Schema elementType = s.getElementType();
      for (Object e : a)
        hashCode = hashCodeAdd(hashCode, e, elementType);
      return hashCode;
    case UNION:
      return hashCode(o, s.getTypes().get(resolveUnion(s, o)));
    case ENUM:
      return s.getEnumOrdinal(o.toString());
    case NULL:
      return 0;
    case STRING:
      return (o instanceof Utf8 ? o : new Utf8(o.toString())).hashCode();
    default:
      return o.hashCode();
    }
  }

  /** Add the hash code for an object into an accumulated hash code. */
  protected int hashCodeAdd(int hashCode, Object o, Schema s) {
    return 31*hashCode + hashCode(o, s);
  }

  /** Compare objects according to their schema.  If equal, return zero.  If
   * greater-than, return 1, if less than return -1.  Order is consistent with
   * that of {@link BinaryData#compare(byte[], int, byte[], int, Schema)}.
   */
  @SuppressWarnings(value="unchecked")
  public int compare(Object o1, Object o2, Schema s) {
    if (o1 == o2) return 0;
    switch (s.getType()) {
    case RECORD:
      for (Field f : s.getFields()) {
        if (f.order() == Field.Order.IGNORE)
          continue;                               // ignore this field
        int pos = f.pos();
        String name = f.name();
        int compare =
          compare(getField(o1, name, pos), getField(o2, name, pos), f.schema());
        if (compare != 0)                         // not equal
          return f.order() == Field.Order.DESCENDING ? -compare : compare;
      }
      return 0;
    case ENUM:
      return s.getEnumOrdinal(o1.toString()) - s.getEnumOrdinal(o2.toString());
    case ARRAY:
      Collection a1 = (Collection)o1;
      Collection a2 = (Collection)o2;
      Iterator e1 = a1.iterator();
      Iterator e2 = a2.iterator();
      Schema elementType = s.getElementType();
      while(e1.hasNext() && e2.hasNext()) {
        int compare = compare(e1.next(), e2.next(), elementType);
        if (compare != 0) return compare;
      }
      return e1.hasNext() ? 1 : (e2.hasNext() ? -1 : 0);
    case MAP:
      throw new AvroRuntimeException("Can't compare maps!");
    case UNION:
      int i1 = resolveUnion(s, o1);
      int i2 = resolveUnion(s, o2);
      return (i1 == i2)
        ? compare(o1, o2, s.getTypes().get(i1))
        : i1 - i2;
    case NULL:
      return 0;
    case STRING:
      Utf8 u1 = o1 instanceof Utf8 ? (Utf8)o1 : new Utf8(o1.toString());
      Utf8 u2 = o2 instanceof Utf8 ? (Utf8)o2 : new Utf8(o2.toString());
      return u1.compareTo(u2);
    default:
      return ((Comparable)o1).compareTo(o2);
    }
  }

}
