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
import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.Decoder;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for existing classes via
 * Java reflection.
 */
public class ReflectDatumReader extends SpecificDatumReader {
  public ReflectDatumReader() {}

  public ReflectDatumReader(Class c) {
    this(ReflectData.get().getSchema(c));
  }

  public ReflectDatumReader(Schema root) {
    super(root);
  }

  @Override
  protected void addField(Object record, String name, int position, Object o) {
    try {
      ReflectData.getField(record.getClass(), name).set(record, o);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  protected Object getField(Object record, String name, int position) {
    try {
      return ReflectData.getField(record.getClass(), name).get(record);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  protected void removeField(Object record, String name, int position) {
    addField(record, name, position, null);
  }

  @Override
  @SuppressWarnings(value="unchecked")
  protected Object newArray(Object old, int size, Schema schema) {
    Class listClass = ReflectData.get().getListClass(schema);
    if (listClass != null) {
      if (old instanceof List) {
        ((List)old).clear();
        return old;
      }
      if (listClass.isAssignableFrom(ArrayList.class))
        return new ArrayList();
      return newInstance(listClass);
    }
    Class elementClass = ReflectData.get().getClass(schema.getElementType());
    return Array.newInstance(elementClass, size);
  }

  @Override
  protected Object peekArray(Object array) {
    return null;
  }

  @Override
  @SuppressWarnings(value="unchecked")
  protected void addToArray(Object array, long pos, Object e) {
    if (array instanceof List) {
      ((List)array).add(e);
    } else {
      Array.set(array, (int)pos, e);
    }
  }

  @Override
  protected Object readString(Object old, Decoder in) throws IOException {
    return super.readString(null, in).toString();
  }

  @Override
  protected Object createString(String value) { return value; }

  @Override
  protected Object readBytes(Object old, Decoder in) throws IOException {
    ByteBuffer bytes = in.readBytes(null);
    byte[] result = new byte[bytes.remaining()];
    bytes.get(result);
    return result;
  }

}

