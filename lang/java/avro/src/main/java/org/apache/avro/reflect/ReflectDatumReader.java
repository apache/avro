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
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for existing classes via
 * Java reflection.
 */
public class ReflectDatumReader<T> extends SpecificDatumReader<T> {
  public ReflectDatumReader() {
    this(null, null, ReflectData.get());
  }

  /** Construct for reading instances of a class. */
  public ReflectDatumReader(Class<T> c) {
    this(new ReflectData(c.getClassLoader()));
    setSchema(getSpecificData().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public ReflectDatumReader(Schema root) {
    this(root, root, ReflectData.get());
  }

  /** Construct given writer's and reader's schema. */
  public ReflectDatumReader(Schema writer, Schema reader) {
    this(writer, reader, ReflectData.get());
  }

  /** Construct given writer's and reader's schema and the data model. */
  public ReflectDatumReader(Schema writer, Schema reader, ReflectData data) {
    super(writer, reader, data);
  }

  /** Construct given a {@link ReflectData}. */
  public ReflectDatumReader(ReflectData data) {
    super(data);
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    Class<?> collectionClass =
      ReflectData.getClassProp(schema, SpecificData.CLASS_PROP);
    Class<?> elementClass =
      ReflectData.getClassProp(schema, SpecificData.ELEMENT_PROP);

    if (collectionClass == null && elementClass == null)
      return super.newArray(old, size, schema);   // use specific/generic

    if (collectionClass != null && !collectionClass.isArray()) {
      if (old instanceof Collection) {
        ((Collection<?>)old).clear();
        return old;
      }
      if (collectionClass.isAssignableFrom(ArrayList.class))
        return new ArrayList<Object>();
      return SpecificData.newInstance(collectionClass, schema);
    }

    if (elementClass == null) {
      ReflectData data = (ReflectData)getData();
      elementClass = data.getClass(schema.getElementType());
    }
    return Array.newInstance(elementClass, size);
  }

  @Override
  protected Object peekArray(Object array) {
    return null;
  }

  @Override
  protected void addToArray(Object array, long pos, Object e) {
    throw new AvroRuntimeException("reflectDatumReader does not use addToArray");
  }

  @Override
  /** Called to read an array instance.  May be overridden for alternate array
   * representations.*/
  protected Object readArray(Object old, Schema expected, ResolvingDecoder in)
      throws IOException {
    Schema expectedType = expected.getElementType();
    long l = in.readArrayStart();
    if (l <= 0) {
      return newArray(old, 0, expected);
    }
    Object array = newArray(old, (int) l, expected);
    if (array instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<Object> c = (Collection<Object>) array;
      return readCollection(c, expectedType, l, in);
    } else {
      return readJavaArray(array, expectedType, l, in);
    }
  }

  private Object readJavaArray(Object array, Schema expectedType, long l,
      ResolvingDecoder in) throws IOException {
    Class<?> elementType = array.getClass().getComponentType();
    if (elementType.isPrimitive()) {
      return readPrimitiveArray(array, elementType, l, in);
    } else {
      return readObjectArray((Object[]) array, expectedType, l, in);
    }
  }

  private Object readPrimitiveArray(Object array, Class<?> c, long l, ResolvingDecoder in)
      throws IOException {
    return ArrayAccessor.readArray(array, c, l, in);
  }

  private Object readObjectArray(Object[] array, Schema expectedType, long l,
      ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      while (index < limit) {
        Object element = read(null, expectedType, in);
        array[index] = element;
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

  private Object readCollection(Collection<Object> c, Schema expectedType,
      long l, ResolvingDecoder in) throws IOException {
    do {
      for (int i = 0; i < l; i++) {
        Object element = read(null, expectedType, in);
        c.add(element);
      }
    } while ((l = in.arrayNext()) > 0);
    return c;
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
    Class<?> c = ReflectData.getClassProp(s, SpecificData.CLASS_PROP);
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
    String intClass = expected.getProp(SpecificData.CLASS_PROP);
    if (Byte.class.getName().equals(intClass))
      value = ((Integer)value).byteValue();
    else if (Short.class.getName().equals(intClass))
      value = ((Integer)value).shortValue();
    else if (Character.class.getName().equals(intClass))
        value = ((Character)(char)(int)(Integer)value);
    return value;
  }

  @Override
  protected void readField(Object record, Field f, Object oldDatum,
      ResolvingDecoder in, Object state) throws IOException {
    if (state != null) {
      FieldAccessor accessor = ((FieldAccessor[]) state)[f.pos()];
      if (accessor != null && !Schema.Type.UNION.equals(f.schema().getType())
          && accessor.supportsIO()) {
        accessor.read(record, in);
        return;
      }
    }
    super.readField(record, f, oldDatum, in, state);
  }
}
