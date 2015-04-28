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
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.ExtendedSpecificDatumWriter;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for existing classes
 * via Java reflection.
 * 1-1 copy from avro lib
 *
 */
public class ExtendedReflectDatumWriter<T> extends ExtendedSpecificDatumWriter<T> {
  public ExtendedReflectDatumWriter() {
    this(ReflectData.get());
  }

  public ExtendedReflectDatumWriter(final Class<T> c) {
    this(c, ReflectData.get());
  }

  public ExtendedReflectDatumWriter(final Class<T> c, final ReflectData data) {
    this(data.getSchema(c), data);
  }

  public ExtendedReflectDatumWriter(final Schema root) {
    this(root, ReflectData.get());
  }

  public ExtendedReflectDatumWriter(final Schema root, final ReflectData reflectData) {
    super(root, reflectData);
  }

  protected ExtendedReflectDatumWriter(final ReflectData reflectData) {
    super(reflectData);
  }

  /** Called to write a array.  May be overridden for alternate array
   * representations.*/
  @Override
  protected final void writeArray(final Schema schema, final Object datum, final Encoder out)
    throws IOException {
    if (datum instanceof Collection) {
      super.writeArray(schema, datum, out);
      return;
    }
    Class<?> elementClass = datum.getClass().getComponentType();
    if (null == elementClass) {
      // not a Collection or an Array
      throw new AvroRuntimeException("Array data must be a Collection or Array, not " + datum);
    }
    Schema element = schema.getElementType();
    if (elementClass.isPrimitive()) {
      Schema.Type type = element.getType();
      out.writeArrayStart();
      switch (type) {
      case BOOLEAN:
        if (elementClass.isPrimitive()) {
            ArrayAccessor.writeArray((boolean[]) datum, out);
        }
        break;
      case DOUBLE:
        ArrayAccessor.writeArray((double[]) datum, out);
        break;
      case FLOAT:
        ArrayAccessor.writeArray((float[]) datum, out);
        break;
      case INT:
        if (elementClass.equals(int.class)) {
          ArrayAccessor.writeArray((int[]) datum, out);
        } else if (elementClass.equals(char.class)) {
          ArrayAccessor.writeArray((char[]) datum, out);
        } else if (elementClass.equals(short.class)) {
          ArrayAccessor.writeArray((short[]) datum, out);
        } else {
          arrayError(elementClass, type);
        }
        break;
      case LONG:
        ArrayAccessor.writeArray((long[]) datum, out);
        break;
      default:
        arrayError(elementClass, type);
      }
      out.writeArrayEnd();
    } else {
      out.writeArrayStart();
      writeObjectArray(element, (Object[]) datum, out);
      out.writeArrayEnd();
    }
  }

  private void writeObjectArray(final Schema element, final Object[] data, final Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (int i = 0; i < size; i++) {
      this.write(element, data[i], out);
    }
  }

  private void arrayError(final Class<?> cl, final Schema.Type type) {
    throw new AvroRuntimeException("Error writing array with inner type " + cl + " and avro type: " + type);
  }

  @Override
  protected final void writeBytes(final Object datum, final Encoder out) throws IOException {
    if (datum instanceof byte[]) {
        out.writeBytes((byte[]) datum);
    } else {
        super.writeBytes(datum, out);
    }
  }

  @Override
  protected final void write(final Schema schema, final Object datum, final Encoder out)
    throws IOException {
      Object realDatum;
    if (datum instanceof Byte) {
        realDatum = ((Byte) datum).intValue();
    } else if (datum instanceof Short) {
        realDatum = ((Short) datum).intValue();
    } else if (datum instanceof Character) {
        realDatum = (int) (char) (Character) datum;
    } else {
        realDatum = datum;
    }

    try {
      super.write(schema, realDatum, out);
    } catch (NullPointerException e) {            // improve error message
      NullPointerException result = new NullPointerException("in " + schema.getFullName() + " " + e.getMessage());
      result.initCause(e.getCause() == null ? e : e.getCause());
      throw result;
    }
  }

  @Override
  protected final void writeField(final Object record, final Field f, final Encoder out, final Object state)
      throws IOException {
    if (state != null) {
      FieldAccessor accessor = ((FieldAccessor[]) state)[f.pos()];
      if (accessor != null) {
        if (accessor.supportsIO()
            && (Schema.Type.UNION != f.schema().getType()
                || accessor.isCustomEncoded())) {
          accessor.write(record, out);
          return;
        }
        if (accessor.isStringable()) {
          try {
            Object object = accessor.get(record);
            write(f.schema(), (object == null) ? null : object.toString(), out);
          } catch (IllegalAccessException e) {
            throw new AvroRuntimeException("Failed to write Stringable field " + f + " for record " + record, e);
          }
          return;
        }
      }
    }
    super.writeField(record, f, out, state);
  }
}
