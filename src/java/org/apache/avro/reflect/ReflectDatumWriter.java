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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for existing classes
 * via Java reflection.
 */
public class ReflectDatumWriter extends GenericDatumWriter<Object> {
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

  public ReflectDatumWriter(Schema root, ReflectData reflectData) {
    super(root, reflectData);
  }
  
  public ReflectDatumWriter(ReflectData reflectData) {
    super(reflectData);
  }
  
  protected Object getField(Object record, String name, int position) {
    try {
      return ReflectData.getField(record.getClass(), name).get(record);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }
  
  protected void writeEnum(Schema schema, Object datum, Encoder out)
    throws IOException {
    out.writeEnum(((Enum)datum).ordinal());
  }

  protected boolean isEnum(Object datum) {
    return datum instanceof Enum;
  }

}

