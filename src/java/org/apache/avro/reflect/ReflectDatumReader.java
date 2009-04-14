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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.ValueReader;

/** {@link DatumReader} for existing classes via Java reflection. */
public class ReflectDatumReader extends GenericDatumReader<Object> {
  protected String packageName;

  public ReflectDatumReader(String packageName) {
    this.packageName = packageName;
  }

  public ReflectDatumReader(Schema root, String packageName) {
    this(packageName);
    setSchema(root);
  }

  protected Object readRecord(Object old, Schema actual, Schema expected,
                              ValueReader in) throws IOException {
    Class recordClass;
    try {
      recordClass = Class.forName(packageName+expected.getName());
    } catch (ClassNotFoundException e) {
      throw new AvroRuntimeException(e);
    }
    expected = ReflectData.getSchema(recordClass);
    Map<String,Schema.Field> expectedFields = expected.getFields();
    Object record = recordClass.isInstance(old) ? old : newInstance(recordClass);
    for (Map.Entry<String, Schema> entry : actual.getFieldSchemas()) {
      try {
        Field field = recordClass.getField(entry.getKey());
        field.setAccessible(true);
        String key = entry.getKey();
        Schema aField = entry.getValue();
        Schema eField = field.getType() ==
          Object.class ? aField : expectedFields.get(key).schema();
        field.set(record, read(null, aField, eField, in));
      } catch (NoSuchFieldException e) {        // ignore unmatched field
      } catch (IllegalAccessException e) {
        throw new AvroRuntimeException(e);
      }
    }
    return record;
  }

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
  private static final Map<Class,Constructor> CTOR_CACHE =
    new ConcurrentHashMap<Class,Constructor>();

  /** Create a new instance of the named class. */
  @SuppressWarnings("unchecked")
  protected static Object newInstance(Class c) {
    Object result;
    try {
      Constructor meth = (Constructor)CTOR_CACHE.get(c);
      if (meth == null) {
        meth = c.getDeclaredConstructor(EMPTY_ARRAY);
        meth.setAccessible(true);
        CTOR_CACHE.put(c, meth);
      }
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  protected void addField(Object record, String name, int position, Object o) {
    throw new AvroRuntimeException("Not implemented");
  }

  @Override
  protected Object getField(Object record, String name, int position) {
    throw new AvroRuntimeException("Not implemented");
  }

  @Override
  protected void removeField(Object record, String field, int position) {
    throw new AvroRuntimeException("Not implemented");
  }

  @Override
  protected Object newRecord(Object old, Schema schema) {
    throw new AvroRuntimeException("Not implemented");
  }
}
