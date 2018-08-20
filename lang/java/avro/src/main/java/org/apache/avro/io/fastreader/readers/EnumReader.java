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
import org.apache.avro.io.Decoder;

public class EnumReader implements FieldReader<Object> {

  private final Object[] enumObjects;
  private final Schema writerSchema;

  public EnumReader(Object[] enumObjects, Schema writerSchema) {
    this.enumObjects = enumObjects;
    this.writerSchema = writerSchema;
  }

  @Override
  public Object read(Object reuse, Decoder decoder) throws IOException {
    int index = decoder.readEnum();
    Object resultObject = enumObjects[index];

    if (resultObject == null) {
      throw new IOException(
          "Non-compatible value '"
              + writerSchema.getEnumSymbols().get(index)
              + "' found in enum "
              + writerSchema.getName());
    }

    return resultObject;
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    decoder.readEnum();
  }
}
