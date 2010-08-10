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
import java.util.Collection;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.Encoder;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for existing classes
 * via Java reflection.
 */
public class ReflectDatumWriter<T> extends SpecificDatumWriter<T> {
  public ReflectDatumWriter() {
    this(ReflectData.get());
  }

  public ReflectDatumWriter(Class<T> c) {
    this(c, ReflectData.get());
  }

  public ReflectDatumWriter(Class<T> c, ReflectData data) {
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
    if (array instanceof Collection)
      return ((Collection)array).size();
    return Array.getLength(array);
        
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Iterator<Object> getArrayElements(final Object array) {
    if (array instanceof Collection)
      return ((Collection<Object>)array).iterator();
    return new Iterator<Object>() {
      private int i = 0;
      private final int length = Array.getLength(array);
      public boolean hasNext() { return i < length; }
      public Object next() { return Array.get(array, i++); }
      public void remove() { throw new UnsupportedOperationException(); }
    };
  }

  @Override
  protected void writeString(Schema schema, Object datum, Encoder out)
    throws IOException {
    if (schema.getProp(ReflectData.CLASS_PROP) != null) // Stringable annotated
      datum = datum.toString();                         // call toString()
    writeString(datum, out);
  }

  @Override
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    out.writeBytes((byte[])datum);
  }

  @Override
  protected void write(Schema schema, Object datum, Encoder out)
    throws IOException {
    if (datum instanceof Short)
      datum = ((Short)datum).intValue();
    try {
      super.write(schema, datum, out);
    } catch (NullPointerException e) {            // improve error message
      NullPointerException result =
        new NullPointerException("in "+schema.getName()+" "+e.getMessage());
      result.initCause(e);
      throw result;
    }
  }

}
