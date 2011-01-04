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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

/** {@link org.apache.avro.io.DatumWriter DatumWriter} for generated Java classes. */
public class SpecificDatumWriter<T> extends GenericDatumWriter<T> {
  public SpecificDatumWriter() {
    super(SpecificData.get());
  }

  public SpecificDatumWriter(Class<T> c) {
    super(SpecificData.get().getSchema(c), SpecificData.get());
  }
  
  public SpecificDatumWriter(Schema schema) {
    super(schema, SpecificData.get());
  }
  
  protected SpecificDatumWriter(Schema root, SpecificData specificData) {
    super(root, specificData);
  }
  
  protected SpecificDatumWriter(SpecificData specificData) {
    super(specificData);
  }
  
  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out)
    throws IOException {
    if (!(datum instanceof Enum))
      super.writeEnum(schema, datum, out);        // punt to generic
    else
      out.writeEnum(((Enum)datum).ordinal());
  }

}

