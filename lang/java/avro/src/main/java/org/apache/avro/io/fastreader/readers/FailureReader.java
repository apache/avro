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
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.fastreader.steps.ExecutionStep;

/**
 * Reader for lazy errors, e.g. trying to read or skip from this will result in an error being
 * thrown. Used in schema conversion of unions with non-matching types.
 */
public class FailureReader implements FieldReader<Object>, ExecutionStep {

  private final String errorMessage;

  public FailureReader(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Override
  public Object read(Object reuse, Decoder decoder) throws IOException {
    throw new IOException(errorMessage);
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    throw new IOException(errorMessage);
  }

  @Override
  public void execute(IndexedRecord record, Decoder decoder) throws IOException {
    throw new IOException(errorMessage);
  }
}
