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
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.ValueWriter;

/** {@link DatumWriter} for existing classes via Java reflection. */
public class ReflectDatumWriter extends GenericDatumWriter<Object> {
  public ReflectDatumWriter() {}

  public ReflectDatumWriter(Schema root) {
    super(root);
  }
  
  protected void writeRecord(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    Class recordClass = datum.getClass();
    for (Map.Entry<String, Schema> entry : schema.getFieldSchemas()) {
      try {
        Field field = recordClass.getField(entry.getKey());
        write(entry.getValue(), field.get(datum), out);
      } catch (NoSuchFieldException e) {
        throw new AvroRuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new AvroRuntimeException(e);
      }
    }
  }
  
  @Override
  protected boolean isRecord(Object datum) {
    return ReflectData.getSchema(datum.getClass()).getType() == Type.RECORD;
  }
  
  @Override
  protected Object getField(Object record, String field, int position) {
    throw new AvroRuntimeException("Not implemented");
  }

  protected boolean instanceOf(Schema schema, Object datum) {
    return (schema.getType() == Type.RECORD)
      ? ReflectData.getSchema(datum.getClass()).getType() == Type.RECORD
      : super.instanceOf(schema, datum);
  }

}

