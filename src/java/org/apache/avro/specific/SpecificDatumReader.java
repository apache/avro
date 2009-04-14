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

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.ValueReader;
import org.apache.avro.reflect.ReflectDatumReader;

/** {@link DatumReader} for generated Java classes. */
public class SpecificDatumReader extends ReflectDatumReader {
  public SpecificDatumReader(String packageName) {
    super(packageName);
  }

  public SpecificDatumReader(Schema root, String packageName) {
    super(root, packageName);
  }

  protected Object newRecord(Object old, Schema schema) {
    Class c = getClass(schema.getName());
    return(c.isInstance(old) ? old : newInstance(c));
  }

  protected void addField(Object record, String name, int position, Object o) {
    ((SpecificRecord)record).set(position, o);
  }
  protected Object getField(Object record, String name, int position) {
    return ((SpecificRecord)record).get(position);
  }
  protected void removeField(Object record, String field, int position) {
    ((SpecificRecord)record).set(position, null);
  }

  private Map<String,Class> classCache = new ConcurrentHashMap<String,Class>();

  private Class getClass(String name) {
    Class c = classCache.get(name);
    if (c == null) {
      try {
        c = Class.forName(packageName + name);
        classCache.put(name, c);
      } catch (ClassNotFoundException e) {
        throw new AvroRuntimeException(e);
      }
    }
    return c;
  }

}
