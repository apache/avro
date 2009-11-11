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
package org.apache.avro.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;

import com.thoughtworks.paranamer.CachingParanamer;
import com.thoughtworks.paranamer.Paranamer;

/** Utilities to use existing Java classes and interfaces via reflection. */
public class ReflectData extends GenericData {
  
  /** {@link ReflectData} implementation that permits null field values.  The
   * schema generated for each field is a union of its declared type and
   * null. */
  public static class AllowNull extends ReflectData {

    private static final AllowNull INSTANCE = new AllowNull();

    /** Return the singleton instance. */
    public static AllowNull get() { return INSTANCE; }

    protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
      Schema schema = super.createFieldSchema(field, names);
      return Schema.createUnion(Arrays.asList(new Schema[] {
            schema,
            Schema.create(Schema.Type.NULL) }));
    }
  }
  
  private static final ReflectData INSTANCE = new ReflectData();

  protected ReflectData() {}
  
  /** Return the singleton instance. */
  public static ReflectData get() { return INSTANCE; }

  @Override
  protected boolean isRecord(Object datum) {
    if (datum == null) return false;
    return getSchema(datum.getClass()).getType() == Type.RECORD;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    return getSchema(record.getClass());
  }

  @Override
  protected boolean isEnum(Object datum) {
    return datum instanceof Enum;
  }

  /** Returns true if an object matches a schema. */
  @Override
  public boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
    case RECORD:
      Class c = datum.getClass(); 
      if (!(datum instanceof Object)) return false;
      for (Map.Entry<String, Schema> entry : schema.getFieldSchemas()) {
        try {
          if (!validate(entry.getValue(),
                        getField(c, entry.getKey()).get(datum)))
          return false;
        } catch (IllegalAccessException e) {
          throw new AvroRuntimeException(e);
        }
      }
      return true;
    case ENUM:
      return datum instanceof Enum
        && schema.getEnumSymbols().contains(((Enum)datum).name());
    case ARRAY:
      if (!(datum instanceof GenericArray)) return false;
      for (Object element : (GenericArray)datum)
        if (!validate(schema.getElementType(), element))
          return false;
      return true;
    case UNION:
      for (Schema type : schema.getTypes())
        if (validate(type, datum))
          return true;
      return false;
    case FIXED:   return datum instanceof GenericFixed;
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

  static Field getField(Class c, String name) {
    try {
      Field f = c.getDeclaredField(name);
      f.setAccessible(true);
      return f;
    } catch (NoSuchFieldException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private Map<String,Class> classCache = new ConcurrentHashMap<String,Class>();

  /** Return the class that implements this schema. */
  public Class getClass(Schema schema) {
    switch (schema.getType()) {
    case FIXED:
    case RECORD:
    case ENUM:
      String full = schema.getFullName();
      Class c = classCache.get(full);
      if (c == null) {
        try {
          c = Class.forName(getClassName(schema));
          classCache.put(full, c);
        } catch (ClassNotFoundException e) {
          throw new AvroRuntimeException(e);
        }
      }
      return c;
    case ARRAY:   return GenericArray.class;
    case MAP:     return Map.class;
    case UNION:   return Object.class;
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

  /** Generate a schema for a Java type.
   * <p>For records, {@link Class#getDeclaredFields() declared fields} (not
   * inherited) which are not static or transient are used.</p>
   * <p>Note that unions cannot be automatically generated by this method,
   * since Java provides no representation for unions.</p>
   */
  public Schema getSchema(java.lang.reflect.Type type) {
    Schema schema = schemaCache.get(type);
    if (schema == null) {
      schema = createSchema(type, new LinkedHashMap<String,Schema>());
      schemaCache.put(type, schema);
    }
    return schema;
  }

  /**
   * Create a schema for a type and it's fields. Note that by design only fields
   * of the direct class, not it's super classes, are used for creating the
   * schema.  Also, fields are not permitted to be null.
   */
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
      } else if (Map.class.isAssignableFrom(raw)) { // map
        java.lang.reflect.Type key = params[0];
        java.lang.reflect.Type value = params[1];
        if (!(key == Utf8.class))
          throw new AvroTypeException("Map key class not Utf8: "+key);
        return Schema.createMap(createSchema(value, names));
      }
    } else if (type instanceof Class) {
      Class c = (Class)type;
      String name = c.getSimpleName();
      String space = c.getPackage().getName();
      if (c.getEnclosingClass() != null)          // nested class
        space = c.getEnclosingClass().getName() + "$";
      String fullName = c.getName();
      Schema schema = names.get(fullName);
      if (schema == null) {

        if (c.isEnum()) {                         // enum
          List<String> symbols = new ArrayList<String>();
          Enum[] constants = (Enum[])c.getEnumConstants();
          for (int i = 0; i < constants.length; i++)
            symbols.add(constants[i].name());
          schema = Schema.createEnum(name, space, symbols);
          names.put(fullName, schema);
          return schema;
        }
                                                  // fixed
        if (GenericFixed.class.isAssignableFrom(c)) {
          int size = ((FixedSize)c.getAnnotation(FixedSize.class)).value();
          schema = Schema.createFixed(name, space, size);
          names.put(fullName, schema);
          return schema;
        }
                                                  // record
        LinkedHashMap<String,Schema.Field> fields =
          new LinkedHashMap<String,Schema.Field>();
        schema = Schema.createRecord(name, space,
                                     Throwable.class.isAssignableFrom(c));
        if (!names.containsKey(fullName))
          names.put(fullName, schema);
        for (Field field : c.getDeclaredFields())
          if ((field.getModifiers()&(Modifier.TRANSIENT|Modifier.STATIC))==0) {
            Schema fieldSchema = createFieldSchema(field, names);
            fields.put(field.getName(), new Schema.Field(fieldSchema, null));
          }
        schema.setFields(fields);
      }
      return schema;
    }
    throw new AvroTypeException("Unknown type: "+type);
  }

  /** Create a schema for a field. */
  protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
    return createSchema(field.getGenericType(), names);
  }

  /** Generate a protocol for a Java interface.
   * <p>Note that this requires that <a
   * href="http://paranamer.codehaus.org/">Paranamer</a> is run over compiled
   * interface declarations, since Java 6 reflection does not provide access to
   * method parameter names.  See Avro's build.xml for an example. </p>
   */
  public Protocol getProtocol(Class iface) {
    Protocol protocol =
      new Protocol(iface.getSimpleName(), iface.getPackage().getName()); 
    Map<String,Schema> names = new LinkedHashMap<String,Schema>();
    for (Method method : iface.getDeclaredMethods())
      if ((method.getModifiers() & Modifier.STATIC) == 0)
        protocol.getMessages().put(method.getName(),
                                   getMessage(method, protocol, names));

    // reverse types, since they were defined in reference order
    List<Schema> types = new ArrayList<Schema>();
    types.addAll(names.values());
    Collections.reverse(types);
    protocol.setTypes(types);

    return protocol;
  }

  private final Paranamer paranamer = new CachingParanamer();

  private Message getMessage(Method method, Protocol protocol,
                             Map<String,Schema> names) {
    LinkedHashMap<String,Schema.Field> fields =
      new LinkedHashMap<String,Schema.Field>();
    String[] paramNames = paranamer.lookupParameterNames(method);
    java.lang.reflect.Type[] paramTypes = method.getGenericParameterTypes();
    for (int i = 0; i < paramTypes.length; i++)
      fields.put(paramNames[i],
                 new Schema.Field(createSchema(paramTypes[i], names), null));
    Schema request = Schema.createRecord(fields);

    Schema response = createSchema(method.getGenericReturnType(), names);

    List<Schema> errs = new ArrayList<Schema>();
    errs.add(Protocol.SYSTEM_ERROR);              // every method can throw
    for (java.lang.reflect.Type err : method.getGenericExceptionTypes())
      if (err != AvroRemoteException.class) 
        errs.add(createSchema(err, names));
    Schema errors = Schema.createUnion(errs);

    return protocol.createMessage(method.getName(), request, response, errors);
  }

}
