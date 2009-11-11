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
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.ipc.AvroRemoteException;

import com.thoughtworks.paranamer.CachingParanamer;
import com.thoughtworks.paranamer.Paranamer;

/** Utilities to use existing Java classes and interfaces via reflection. */
public class ReflectData extends SpecificData {
  
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
    default:
      return super.validate(schema, datum);
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

  /** Create a schema for a Java class.  Note that by design only fields of the
   * direct class, not it's super classes, are used for creating a record
   * schema.  Also, fields are not permitted to be null.  {@link
   * Class#getDeclaredFields() Declared fields} (not inherited) which are not
   * static or transient are used.*/
  @Override
  @SuppressWarnings(value="unchecked")
  protected Schema createClassSchema(Class c, Map<String,Schema> names) {
    String name = c.getSimpleName();
    String space = c.getPackage().getName();
    if (c.getEnclosingClass() != null)                   // nested class
      space = c.getEnclosingClass().getName() + "$";
    Schema schema;
    if (c.isEnum()) {                                    // enum
      List<String> symbols = new ArrayList<String>();
      Enum[] constants = (Enum[])c.getEnumConstants();
      for (int i = 0; i < constants.length; i++)
        symbols.add(constants[i].name());
      schema = Schema.createEnum(name, space, symbols);
    } else if (GenericFixed.class.isAssignableFrom(c)) { // fixed
      int size = ((FixedSize)c.getAnnotation(FixedSize.class)).value();
      schema = Schema.createFixed(name, space, size);
    } else {                                             // record
      LinkedHashMap<String,Schema.Field> fields =
        new LinkedHashMap<String,Schema.Field>();
      schema = Schema.createRecord(name, space,
                                   Throwable.class.isAssignableFrom(c));
      names.put(c.getName(), schema);
      for (Field field : c.getDeclaredFields())
        if ((field.getModifiers()&(Modifier.TRANSIENT|Modifier.STATIC))==0) {
          Schema fieldSchema = createFieldSchema(field, names);
          fields.put(field.getName(), new Schema.Field(fieldSchema, null));
        }
      schema.setFields(fields);
    }
    return schema;
  }

  /** Create a schema for a field. */
  protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
    return createSchema(field.getGenericType(), names);
  }

  /** Return the protocol for a Java interface.
   * <p>Note that this requires that <a
   * href="http://paranamer.codehaus.org/">Paranamer</a> is run over compiled
   * interface declarations, since Java 6 reflection does not provide access to
   * method parameter names.  See Avro's build.xml for an example. */
  @Override
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

  @Override
  public int compare(Object o1, Object o2, Schema s) {
    throw new UnsupportedOperationException();
  }


}
