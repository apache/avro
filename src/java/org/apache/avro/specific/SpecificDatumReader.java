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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumReader;

/** {@link DatumReader} for generated Java classes. */
public class SpecificDatumReader extends ReflectDatumReader {
  public SpecificDatumReader(String packageName) {
    super(packageName);
  }

  public SpecificDatumReader(Schema root, String packageName) {
    super(root, packageName);
  }

  protected Object readRecord(Object old, Schema remote, Schema local,
                              ValueReader in) throws IOException {
    Class c = getClass(remote.getName());
    SpecificRecord record =
      (SpecificRecord)(c.isInstance(old) ? old : newInstance(c));
    local = record.schema();
    Map<String,Schema> localFields = local.getFields();
    int[] map = getMap(local, remote);
    int i = 0, size = 0, j = 0;
    for (Map.Entry<String,Schema> entry : remote.getFields().entrySet()) {
      String key = entry.getKey();
      Schema rField = entry.getValue();
      Schema lField = local == remote ? rField : localFields.get(key);
      int fieldNum = map[i++];
      if (fieldNum == -1) {
        skip(rField, in);
        continue;
      }
      Object oldDatum = old != null ? record.get(fieldNum) : null;
      record.set(fieldNum, read(oldDatum, rField, lField, in));
      size++;
    }
    if (local.getFields().size() > size)          // clear unset fields
      for (Map.Entry<String,Schema> entry : local.getFields().entrySet()) {
        if (!(remote.getFields().containsKey(entry.getKey()) &&
              local.getFields().containsKey(entry.getKey())))
          record.set(j, null);
        j++;
      }
    return record;
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

  private Map<Schema,Map<Schema,int[]>> mapCache =
    new IdentityHashMap<Schema,Map<Schema,int[]>>();

  private int[] getMap(Schema local, Schema remote) {
    synchronized (mapCache) {
      Map<Schema,int[]> localCache = mapCache.get(local);
      if (localCache == null) {
        localCache = new IdentityHashMap<Schema,int[]>();
        mapCache.put(local, localCache);
      }
      int[] result = localCache.get(remote);
      if (result == null) {
        result = createMap(remote, local);
        localCache.put(remote, result);
      }
      return result;
    }
  }

  private static int[] createMap(Schema remote, Schema local) {
    int[] map = new int[remote.getFields().size()];
    int i = 0;
    for (Map.Entry<String,Schema> f : remote.getFields().entrySet()) {
      map[i++] = getLocalIndex(f.getKey(), f.getValue().getType(), local);
    }
    return map;
  }

  private static int getLocalIndex(String name, Schema.Type type,
                                   Schema local) {
    int i = 0;    
    for (Map.Entry<String,Schema> f : local.getFields().entrySet()) {
      if (f.getKey().equals(name) && f.getValue().getType().equals(type))
        return i;
      i++;
    }
    return -1;
  }

}
