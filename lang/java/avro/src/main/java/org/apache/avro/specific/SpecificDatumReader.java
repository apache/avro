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
package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;

/** {@link org.apache.avro.io.DatumReader DatumReader} for generated Java classes. */
public class SpecificDatumReader<T> extends GenericDatumReader<T> {
  public SpecificDatumReader() {
    this(null, null, SpecificData.get());
  }

  public SpecificDatumReader(Class<T> c) {
    this(SpecificData.get().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public SpecificDatumReader(Schema schema) {
    this(schema, schema, SpecificData.get());
  }

  /** Construct given writer's and reader's schema. */
  public SpecificDatumReader(Schema writer, Schema reader) {
    this(writer, reader, SpecificData.get());
  }

  /** Construct given writer's schema, reader's schema, and a {@link
   * SpecificData}. */
  public SpecificDatumReader(Schema writer, Schema reader,
                             SpecificData data) {
    super(writer, reader, data);
  }

  /** Return the contained {@link SpecificData}. */
  public SpecificData getSpecificData() { return (SpecificData)getData(); }

  @Override
  @SuppressWarnings("unchecked")
  protected Object createEnum(String symbol, Schema schema) {
    Class c = getSpecificData().getClass(schema);
    if (c == null) return super.createEnum(symbol, schema); // punt to generic
    return Enum.valueOf(c, symbol);
  }

}

