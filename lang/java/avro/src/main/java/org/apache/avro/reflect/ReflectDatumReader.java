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
import java.util.Collection;
import java.util.ArrayList;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.Decoder;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for existing classes via
 * Java reflection.
 */
public class ReflectDatumReader<T> extends SpecificDatumReader<T> {
  public ReflectDatumReader() {
    this(null, null, ReflectData.get());
  }

  public ReflectDatumReader(Class<T> c) {
    this(ReflectData.get().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public ReflectDatumReader(Schema root) {
    this(root, root, ReflectData.get());
  }

  /** Construct given writer's and reader's schema. */
  public ReflectDatumReader(Schema writer, Schema reader) {
    this(writer, reader, ReflectData.get());
  }

  protected ReflectDatumReader(Schema writer, Schema reader, ReflectData data){
    super(writer, reader, data);
  }

  @Override
  @SuppressWarnings(value="unchecked")
  protected Object newArray(Object old, int size, Schema schema) {
    Class collectionClass =
      ReflectData.getClassProp(schema, ReflectData.CLASS_PROP);
    Class elementClass =
      ReflectData.getClassProp(schema, ReflectData.ELEMENT_PROP);

    if (collectionClass == null && elementClass == null)
      return super.newArray(old, size, schema);   // use specific/generic

    ReflectData data = (ReflectData)getData();
    if (collectionClass != null && !collectionClass.isArray()) {
      if (old instanceof Collection) {
        ((Collection)old).clear();
        return old;
      }
      if (collectionClass.isAssignableFrom(ArrayList.class))
        return new ArrayList();
      return data.newInstance(collectionClass, schema);
    }

    if (elementClass == null)
      elementClass = data.getClass(schema.getElementType());
    return Array.newInstance(elementClass, size);
  }

  @Override
  protected Object peekArray(Object array) {
    return null;
  }

  @Override
  @SuppressWarnings(value="unchecked")
  protected void addToArray(Object array, long pos, Object e) {
    if (array instanceof Collection) {
      ((Collection)array).add(e);
    } else {
      Array.set(array, (int)pos, e);
    }
  }

  @Override
  @SuppressWarnings(value="unchecked")
  protected Object readString(Object old, Schema s,
                              Decoder in) throws IOException {
    String value = (String)readString(null, in);
    Class c = ReflectData.getClassProp(s, ReflectData.CLASS_PROP);
    if (c != null)                                // Stringable annotated class
      try {                                       // use String-arg ctor
        return c.getConstructor(String.class).newInstance(value);
      } catch (NoSuchMethodException e) {
        throw new AvroRuntimeException(e);
      } catch (InstantiationException e) {
        throw new AvroRuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new AvroRuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new AvroRuntimeException(e);
      }
    return value;
  }

  @Override
  protected Object readString(Object old, Decoder in) throws IOException {
    return super.readString(null, in).toString();
  }

  @Override
  protected Object createString(String value) { return value; }

  @Override
  protected Object readBytes(Object old, Schema s, Decoder in)
    throws IOException {
    ByteBuffer bytes = in.readBytes(null);
    Class c = ReflectData.getClassProp(s, ReflectData.CLASS_PROP);
    if (c != null && c.isArray()) {
      byte[] result = new byte[bytes.remaining()];
      bytes.get(result);
      return result;
    } else {
      return bytes;
    }
  }

  @Override
  protected Object readInt(Object old,
                           Schema expected, Decoder in) throws IOException {
    Object value = in.readInt();
    String intClass = expected.getProp(ReflectData.CLASS_PROP);
    if (Byte.class.getName().equals(intClass))
      value = ((Integer)value).byteValue();
    else if (Short.class.getName().equals(intClass))
      value = ((Integer)value).shortValue();
    return value;
  }

}
