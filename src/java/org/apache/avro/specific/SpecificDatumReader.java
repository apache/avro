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

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;

/** {@link org.apache.avro.io.DatumReader DatumReader} for generated Java classes. */
public class SpecificDatumReader extends GenericDatumReader<Object> {
  public SpecificDatumReader() {}

  public SpecificDatumReader(Class c) {
    this(SpecificData.get().getSchema(c));
  }

  public SpecificDatumReader(Schema schema) {
    super(schema);
  }

  @Override
  protected Object newRecord(Object old, Schema schema) {
    Class c = SpecificData.get().getClass(schema);
    return (c.isInstance(old) ? old : newInstance(c));
  }

  @Override
  protected void addField(Object record, String name, int position, Object o) {
    ((SpecificRecord)record).set(position, o);
  }
  @Override
  protected Object getField(Object record, String name, int position) {
    return ((SpecificRecord)record).get(position);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Object createEnum(String symbol, Schema schema) {
    return Enum.valueOf(SpecificData.get().getClass(schema), symbol);
  }

  @Override
  protected Object createFixed(Object old, Schema schema) {
    Class c = SpecificData.get().getClass(schema);
    return c.isInstance(old) ? old : newInstance(c);
  }

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
  private static final Map<Class,Constructor> CTOR_CACHE =
    new ConcurrentHashMap<Class,Constructor>();

  /** Create an instance of a class. */
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

}

