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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.ExtendedSpecificDatumReader;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for existing classes via
 * Java reflection.
 *
 * THis has been 1-1 replicated from apache avro due to inheritance design
 */
//CHECKSTYLE IGNORE DesignForExtension FOR NEXT 900 LINES
public class ExtendedReflectDatumReader<T> extends ExtendedSpecificDatumReader<T> {
  public ExtendedReflectDatumReader() {
    this(null, null, ReflectData.get());
  }

  /** Construct for reading instances of a class. */
  public ExtendedReflectDatumReader(final Class<T> c) {
    this(new ReflectData(c.getClassLoader()));
    setSchema(getSpecificData().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public ExtendedReflectDatumReader(final Schema root) {
    this(root, root, ReflectData.get());
  }

  /** Construct given writer's and reader's schema. */
  public ExtendedReflectDatumReader(final Schema writer, final Schema reader) {
    this(writer, reader, ReflectData.get());
  }

  /** Construct given writer's and reader's schema and the data model. */
  public ExtendedReflectDatumReader(final Schema writer, final Schema reader, final ReflectData data) {
    super(writer, reader, data);
  }

  /** Construct given a {@link ReflectData}. */
  public ExtendedReflectDatumReader(final ReflectData data) {
    super(data);
  }

  @Override
  protected Object newArray(final Object old, final int size, final Schema schema) {
    Class<?> collectionClass =
      ReflectData.getClassProp(schema, SpecificData.CLASS_PROP);
    Class<?> elementClass =
      ReflectData.getClassProp(schema, SpecificData.ELEMENT_PROP);

    if (collectionClass == null && elementClass == null) {
        return super.newArray(old, size, schema);   // use specific/generic
    }

    if (collectionClass != null && !collectionClass.isArray()) {
      if (old instanceof Collection) {
        ((Collection<?>) old).clear();
        return old;
      }
      if (collectionClass.isAssignableFrom(ArrayList.class)) {
          return new ArrayList<Object>();
      }
      return SpecificData.newInstance(collectionClass, schema);
    }

    if (elementClass == null) {
      elementClass = collectionClass.getComponentType();
    }
    if (elementClass == null) {
      ReflectData data = (ReflectData) getData();
      elementClass = data.getClass(schema.getElementType());
    }
    return Array.newInstance(elementClass, size);
  }

  @Override
  protected Object peekArray(final Object array) {
    return null;
  }

  @Override
  protected void addToArray(final Object array, final long pos, final Object e) {
    throw new AvroRuntimeException("reflectDatumReader does not use addToArray for " + array + ", " + pos + ", " + e);
  }

  @Override
  /** Called to read an array instance.  May be overridden for alternate array
   * representations.*/
  protected Object readArray(final Object old, final Schema expected, final ResolvingDecoder in)
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

  private Object readJavaArray(final Object array, final Schema expectedType, final long l,
                               final ResolvingDecoder in) throws IOException {
    Class<?> elementType = array.getClass().getComponentType();
    if (elementType.isPrimitive()) {
      return readPrimitiveArray(array, elementType, l, in);
    } else {
      return readObjectArray((Object[]) array, expectedType, l, in);
    }
  }

  private Object readPrimitiveArray(final Object array, final Class<?> c, final long l, final ResolvingDecoder in)
      throws IOException {
    return ArrayAccessor.readArray(array, c, l, in);
  }

  private Object readObjectArray(final Object[] array, final Schema expectedType, final long length,
                                 final ResolvingDecoder in) throws IOException {
    int index = 0;
    long l = length;
    do {
      int limit = index + (int) l;
      while (index < limit) {
        Object element = read(null, expectedType, in);
        array[index] = element;
        index++;
      }
      l = in.arrayNext();
    } while (l > 0);
    return array;
  }

  private Object readCollection(final Collection<Object> c, final Schema expectedType,
                                final long length, final ResolvingDecoder in) throws IOException {
    long l = length;
    do {
      for (int i = 0; i < l; i++) {
        Object element = read(null, expectedType, in);
        c.add(element);
      }
        l = in.arrayNext();
    } while (l > 0);
    return c;
  }

  @Override
  protected Object readString(final Object old, final Decoder in) throws IOException {
    return super.readString(null, in).toString();
  }

  @Override
  protected Object createString(final String value) {
      return value;
  }

  @Override
  protected Object readBytes(final Object old, final Schema s, final Decoder in)
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
  protected Object readInt(final Object old,
                           final Schema expected, final Decoder in) throws IOException {
    Object value = in.readInt();
    String intClass = expected.getProp(SpecificData.CLASS_PROP);
    if (Byte.class.getName().equals(intClass)) {
        value = ((Integer) value).byteValue();
    } else if (Short.class.getName().equals(intClass)) {
        value = ((Integer) value).shortValue();
    } else if (Character.class.getName().equals(intClass)) {
        value = ((Character) (char) (int) (Integer) value);
    }
    return value;
  }

  @Override
  protected void readField(final Object record, final Field f, final Object oldDatum,
                           final ResolvingDecoder in, final Object state) throws IOException {
    if (state != null) {
      FieldAccessor accessor = ((FieldAccessor[]) state)[f.pos()];
      if (accessor != null) {
        if (accessor.supportsIO()
            && (Schema.Type.UNION != f.schema().getType()
                || accessor.isCustomEncoded())) {
          accessor.read(record, in);
          return;
        }
        if (accessor.isStringable()) {
          try {
            String asString = (String) read(null, f.schema(), in);
            accessor.set(record, asString == null
              ? null
              : newInstanceFromString(accessor.getField().getType(), asString));
            return;
          } catch (RuntimeException e) {
            throw new AvroRuntimeException("Failed to read Stringable field " + f + ", for record " + record, e);
          } catch (IllegalAccessException e) {
            throw new AvroRuntimeException("Failed to read Stringable field " + f + ", for record " + record, e);
          }
        }
      }
    }
    super.readField(record, f, oldDatum, in, state);
  }
}
