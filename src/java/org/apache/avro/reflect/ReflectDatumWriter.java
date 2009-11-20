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

import java.lang.reflect.Array;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for existing classes
 * via Java reflection.
 */
public class ReflectDatumWriter extends SpecificDatumWriter {
  public ReflectDatumWriter() {
    this(ReflectData.get());
  }

  public ReflectDatumWriter(Class c) {
    this(c, ReflectData.get());
  }

  public ReflectDatumWriter(Class c, ReflectData data) {
    this(data.getSchema(c), data);
  }

  public ReflectDatumWriter(Schema root) {
    this(root, ReflectData.get());
  }

  protected ReflectDatumWriter(Schema root, ReflectData reflectData) {
    super(root, reflectData);
  }
  
  protected ReflectDatumWriter(ReflectData reflectData) {
    super(reflectData);
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
  @SuppressWarnings("unchecked")
  protected long getArraySize(Object array) {
    if (array instanceof List)
      return ((List)array).size();
    return Array.getLength(array);
        
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Iterator<Object> getArrayElements(final Object array) {
    if (array instanceof List)
      return ((List<Object>)array).iterator();
    return new Iterator<Object>() {
      private int i = 0;
      private final int length = Array.getLength(array);
      public boolean hasNext() { return i < length; }
      public Object next() { return Array.get(array, i++); }
      public void remove() { throw new UnsupportedOperationException(); }
    };
  }

  @Override
  protected void writeString(Object datum, Encoder out) throws IOException {
    out.writeString(new Utf8((String)datum));
  }

  @Override
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    out.writeBytes((byte[])datum);
  }


}


