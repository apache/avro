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
package org.apache.avro.specific;

import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.nio.ByteBuffer;
import java.lang.reflect.ParameterizedType;

import org.apache.avro.Schema;
import org.apache.avro.Protocol;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;

/** Utilities for generated Java classes and interfaces. */
public class SpecificData extends GenericData {

  private static final SpecificData INSTANCE = new SpecificData();

  protected SpecificData() {}
  
  /** Return the singleton instance. */
  public static SpecificData get() { return INSTANCE; }

  @Override
  protected boolean isRecord(Object datum) {
    return datum instanceof SpecificRecord;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    return ((SpecificRecord)record).getSchema();
  }

  @Override
  protected boolean isEnum(Object datum) {
    return datum instanceof Enum;
  }

  @Override
  public boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      Class c = datum.getClass(); 
      if (!(datum instanceof SpecificRecord)) return false;
      SpecificRecord record = (SpecificRecord)datum;
      Iterator<Field> fields = schema.getFields().values().iterator();
      for (int i = 0; fields.hasNext(); i++)
        if (!validate(fields.next().schema(), record.get(i)))
          return false;
      return true;
    case ENUM:
      return datum instanceof Enum
        && schema.getEnumSymbols().contains(((Enum)datum).name());
    default:
      return super.validate(schema, datum);
    }
  }

  private Map<String,Class> classCache = new ConcurrentHashMap<String,Class>();

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  /** Return the class that implements a schema. */
  public Class getClass(Schema schema) {
    switch (schema.getType()) {
    case FIXED:
    case RECORD:
    case ENUM:
      String name = schema.getFullName();
      Class c = classCache.get(name);
      if (c == null) {
        try {
          c = Class.forName(getClassName(schema));
          classCache.put(name, c);
        } catch (ClassNotFoundException e) {
          throw new AvroRuntimeException(e);
        }
      }
      return c;
    case ARRAY:   return GenericArray.class;
    case MAP:     return Map.class;
    case UNION:
      List<Schema> types = schema.getTypes();     // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return getClass(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return Object.class;
    case STRING:  return Utf8.class;
    case BYTES:   return ByteBuffer.class;
    case INT:     return Integer.TYPE;
    case LONG:    return Long.TYPE;
    case FLOAT:   return Float.TYPE;
    case DOUBLE:  return Double.TYPE;
    case BOOLEAN: return Boolean.TYPE;
    case NULL:    return Void.TYPE;
    default: throw new AvroRuntimeException("Unknown type: "+schema);
    }
  }

  /** Returns the Java class name indicated by a schema's name and namespace. */
  public String getClassName(Schema schema) {
    String namespace = schema.getNamespace();
    String name = schema.getName();
    if (namespace == null)
      return name;
    String dot = namespace.endsWith("$") ? "" : ".";
    return namespace + dot + name;
  }

  private final WeakHashMap<java.lang.reflect.Type,Schema> schemaCache =
    new WeakHashMap<java.lang.reflect.Type,Schema>();

  /** Find the schema for a Java type. */
  public Schema getSchema(java.lang.reflect.Type type) {
    Schema schema = schemaCache.get(type);
    if (schema == null) {
      schema = createSchema(type, new LinkedHashMap<String,Schema>());
      schemaCache.put(type, schema);
    }
    return schema;
  }

  /** Create the schema for a Java type. */
  @SuppressWarnings(value="unchecked")
  protected Schema createSchema(java.lang.reflect.Type type,
                                Map<String,Schema> names) {
    if (type == Utf8.class)
      return Schema.create(Type.STRING);
    else if (type == ByteBuffer.class)
      return Schema.create(Type.BYTES);
    else if ((type == Integer.class) || (type == Integer.TYPE))
      return Schema.create(Type.INT);
    else if ((type == Long.class) || (type == Long.TYPE))
      return Schema.create(Type.LONG);
    else if ((type == Float.class) || (type == Float.TYPE))
      return Schema.create(Type.FLOAT);
    else if ((type == Double.class) || (type == Double.TYPE))
      return Schema.create(Type.DOUBLE);
    else if ((type == Boolean.class) || (type == Boolean.TYPE))
      return Schema.create(Type.BOOLEAN);
    else if ((type == Void.class) || (type == Void.TYPE))
      return Schema.create(Type.NULL);
    else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      Class raw = (Class)ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      for (int i = 0; i < params.length; i++)
      if (GenericArray.class.isAssignableFrom(raw)) { // array
        if (params.length != 1)
          throw new AvroTypeException("No array type specified.");
        return Schema.createArray(createSchema(params[0], names));
      } else if (Map.class.isAssignableFrom(raw)) {   // map
        java.lang.reflect.Type key = params[0];
        java.lang.reflect.Type value = params[1];
        if (!(key == Utf8.class))
          throw new AvroTypeException("Map key class not Utf8: "+key);
        return Schema.createMap(createSchema(value, names));
      }
    } else if (type instanceof Class) {               // class
      Class c = (Class)type;
      String fullName = c.getName();
      Schema schema = names.get(fullName);
      if (schema == null)
        try {
          schema = (Schema)(c.getDeclaredField("SCHEMA$").get(null));
        } catch (NoSuchFieldException e) {
          throw new AvroRuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new AvroRuntimeException(e);
        }
      names.put(fullName, schema);
      return schema;
    }
    throw new AvroTypeException("Unknown type: "+type);
  }

  /** Return the protocol for a Java interface. */
  public Protocol getProtocol(Class iface) {
    try {
      return (Protocol)(iface.getDeclaredField("PROTOCOL").get(null));
    } catch (NoSuchFieldException e) {
      throw new AvroRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public int hashCode(Object o, Schema s) {
    switch (s.getType()) {
    case RECORD:
      int hashCode = 1;
      SpecificRecord r = (SpecificRecord)o;
      Iterator<Field> fields = s.getFields().values().iterator();
      for (int i = 0; fields.hasNext(); i++) {
        Field f = fields.next();
        if (f.order() == Field.Order.IGNORE)
          continue;
        hashCode = hashCodeAdd(hashCode, r.get(i), f.schema());
      }
      return hashCode;
    default:
      return super.hashCode(o, s);
    }
  }

  @Override
  public int compare(Object o1, Object o2, Schema s) {
    switch (s.getType()) {
    case RECORD:
      SpecificRecord r1 = (SpecificRecord)o1;
      SpecificRecord r2 = (SpecificRecord)o2;
      Iterator<Field> fields = s.getFields().values().iterator();
      for (int i = 0; fields.hasNext(); i++) {
        Field f = fields.next();
        if (f.order() == Field.Order.IGNORE)
          continue;                               // ignore this field
        int compare = compare(r1.get(i), r2.get(i), f.schema());
        if (compare != 0)                         // not equal
          return f.order() == Field.Order.DESCENDING ? -compare : compare;
      }
      return 0;
    case ENUM:
      return ((Enum)o1).ordinal() - ((Enum)o2).ordinal();
    default:
      return super.compare(o1, o2, s);
    }
  }

}




