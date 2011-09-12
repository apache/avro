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

import java.util.Map;
import java.util.Collection;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.nio.ByteBuffer;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;

import org.apache.avro.Schema;
import org.apache.avro.Protocol;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;

/** Utilities for generated Java classes and interfaces. */
public class SpecificData extends GenericData {

  private static final SpecificData INSTANCE = new SpecificData();
  
  private final ClassLoader classLoader;
  
  private static final Class<?>[] NO_ARG = new Class[]{};
  private static final Class<?>[] SCHEMA_ARG = new Class[]{Schema.class};
  private static final Map<Class,Constructor> CTOR_CACHE =
    new ConcurrentHashMap<Class,Constructor>();

  /** For subclasses.  Applications normally use {@link SpecificData#get()}. */
  protected SpecificData() { this(SpecificData.class.getClassLoader()); }

  /** Construct with a specific classloader. */
  public SpecificData(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }
  
  /** Return the singleton instance. */
  public static SpecificData get() { return INSTANCE; }

  @Override
  protected boolean isEnum(Object datum) {
    return datum instanceof Enum || super.isEnum(datum);
  }

  @Override
  protected Schema getEnumSchema(Object datum) {
    return (datum instanceof Enum)
      ? getSchema(datum.getClass())
      : super.getEnumSchema(datum);
  }

  private Map<String,Class> classCache = new ConcurrentHashMap<String,Class>();

  private static final Class NO_CLASS = new Object(){}.getClass();
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  /** Return the class that implements a schema, or null if none exists. */
  public Class getClass(Schema schema) {
    switch (schema.getType()) {
    case FIXED:
    case RECORD:
    case ENUM:
      String name = schema.getFullName();
      if (name == null) return null;
      Class c = classCache.get(name);
      if (c == null) {
        try {
          c = classLoader.loadClass(getClassName(schema));
        } catch (ClassNotFoundException e) {
          c = NO_CLASS;
        }
        classCache.put(name, c);
      }
      return c == NO_CLASS ? null : c;
    case ARRAY:   return List.class;
    case MAP:     return Map.class;
    case UNION:
      List<Schema> types = schema.getTypes();     // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return getClass(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return Object.class;
    case STRING:  return CharSequence.class;
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
  public static String getClassName(Schema schema) {
    String namespace = schema.getNamespace();
    String name = schema.getName();
    if (namespace == null || "".equals(namespace))
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
    if (type instanceof Class
        && CharSequence.class.isAssignableFrom((Class)type))
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
      if (Collection.class.isAssignableFrom(raw)) { // array
        if (params.length != 1)
          throw new AvroTypeException("No array type specified.");
        return Schema.createArray(createSchema(params[0], names));
      } else if (Map.class.isAssignableFrom(raw)) {   // map
        java.lang.reflect.Type key = params[0];
        java.lang.reflect.Type value = params[1];
        if (!(type instanceof Class
              && CharSequence.class.isAssignableFrom((Class)type)))
          throw new AvroTypeException("Map key class not CharSequence: "+key);
        return Schema.createMap(createSchema(value, names));
      } else {
        return createSchema(raw, names);
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
  protected int compare(Object o1, Object o2, Schema s, boolean eq) {
    switch (s.getType()) {
    case ENUM:
      if (o1 instanceof Enum)
        return ((Enum)o1).ordinal() - ((Enum)o2).ordinal();
    default:
      return super.compare(o1, o2, s, eq);
    }
  }
  
  /** Create an instance of a class.  If the class implements {@link
   * SchemaConstructable}, call a constructor with a {@link
   * org.apache.avro.Schema} parameter, otherwise use a no-arg constructor. */
  @SuppressWarnings("unchecked")
  public static Object newInstance(Class c, Schema s) {
    boolean useSchema = SchemaConstructable.class.isAssignableFrom(c);
    Object result;
    try {
      Constructor meth = (Constructor)CTOR_CACHE.get(c);
      if (meth == null) {
        meth = c.getDeclaredConstructor(useSchema ? SCHEMA_ARG : NO_ARG);
        meth.setAccessible(true);
        CTOR_CACHE.put(c, meth);
      }
      result = meth.newInstance(useSchema ? new Object[]{s} : (Object[])null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }
  
  @Override
  public Object createFixed(Object old, Schema schema) {
    Class c = SpecificData.get().getClass(schema);
    if (c == null) return super.createFixed(old, schema); // punt to generic
    return c.isInstance(old) ? old : newInstance(c, schema);
  }
  
  @Override
  public Object newRecord(Object old, Schema schema) {
    Class c = SpecificData.get().getClass(schema);
    if (c == null) return super.newRecord(old, schema); // punt to generic
    return (c.isInstance(old) ? old : newInstance(c, schema));
  }

  /** Tag interface that indicates that a class has a one-argument constructor
   * that accepts a Schema.
   * @see SpecificDatumReader#newInstance
   */
  public interface SchemaConstructable {}
  
}
