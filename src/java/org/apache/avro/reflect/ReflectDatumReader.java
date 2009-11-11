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
import org.apache.avro.specific.SpecificDatumReader;

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

}

