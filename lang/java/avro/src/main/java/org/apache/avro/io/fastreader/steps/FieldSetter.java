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
package org.apache.avro.io.fastreader.steps;

import java.io.IOException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.fastreader.readers.FieldReader;

public class FieldSetter<D> implements ExecutionStep {

  private final int fieldNumber;
  private final boolean reuse;
  private final FieldReader<D> reader;

  public FieldSetter(int fieldNumber, FieldReader<D> reader) {
    this.fieldNumber = fieldNumber;
    this.reader = reader;
    this.reuse = reader.canReuse();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(IndexedRecord record, Decoder decoder) throws IOException {
    D reusableObject = reuse ? (D) record.get(fieldNumber) : null;
    record.put(fieldNumber, reader.read(reusableObject, decoder));
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    reader.skip(decoder);
  }
}
