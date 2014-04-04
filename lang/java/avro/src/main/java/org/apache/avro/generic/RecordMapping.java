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
package org.apache.avro.generic;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

/**
 * Expert: a custom mapping that writes an object directly as an Avro record.
 * No validation is performed to check that the encoding conforms to the schema.
 * Invalid implementations may result in an unreadable file.
 * The use of {@link org.apache.avro.io.ValidatingEncoder} is recommended.
 *
 * @param <T> The class of objects that can be serialized as Avro records using this
 *           record mapping.
 * @see org.apache.avro.generic.GenericData#addRecordMapping(RecordMapping)
 */
public abstract class RecordMapping<T> {

  private final Schema schema;
  private final Class<T> recordClass;

  /**
   * Create a record mapping for the given record {@link org.apache.avro.Schema}
   * for serializing objects of the given {@link java.lang.Class}.
   */
  public RecordMapping(Schema schema, Class<T> recordClass) {
    this.schema = schema;
    this.recordClass = recordClass;
  }

  /**
   * @return the schema describing the records that are serialized using this record
   * mapping.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * @return the class of objects that can be serialized as Avro records using this
   * record mapping.
   */
  public Class<T> getRecordClass() {
    return recordClass;
  }

  public abstract void write(Object datum, Encoder out) throws IOException;

  public abstract T read(Object reuse, Decoder in) throws IOException;

}
