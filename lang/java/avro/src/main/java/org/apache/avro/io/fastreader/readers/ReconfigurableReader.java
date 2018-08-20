/*
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
package org.apache.avro.io.fastreader.readers;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.fastreader.FastReader;

public class ReconfigurableReader<T> implements DatumReader<T> {

  private final FastReader fastData;
  private final Schema readerSchema;
  private Schema writerSchema;
  private FieldReader<T> reader;


  public ReconfigurableReader(FastReader fastData, Schema readerSchema, Schema writerSchema) {
    this.fastData = fastData;
    this.readerSchema = readerSchema;
    setSchema(writerSchema);
  }


  @Override
  public void setSchema(Schema schema) {
    this.writerSchema = schema;
    this.reader = null;
  }

  @Override
  public T read(T reuse, Decoder in) throws IOException {
    return getReader().read(reuse, in);
  }

  @SuppressWarnings("unchecked")
  private FieldReader<T> getReader() {
    if (reader == null) {
      reader = (FieldReader<T>) fastData.createDatumReader(writerSchema, readerSchema);
    }
    return reader;
  }
}
