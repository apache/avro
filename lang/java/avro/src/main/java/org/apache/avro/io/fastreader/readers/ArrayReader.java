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
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;

public class ArrayReader<D> implements FieldReader<List<D>> {

  private final FieldReader<D> elementReader;
  private final Schema schema;

  public static <D> ArrayReader<D> of( FieldReader<D> elementReader, Schema schema ) {
    return elementReader.canReuse() ? new ReusingArrayReader<>( elementReader, schema ) : new ArrayReader<>( elementReader, schema );
  }

  public ArrayReader(FieldReader<D> elementReader, Schema schema ) {
    this.elementReader = elementReader;
    this.schema = schema;
  }

  public FieldReader<D> getElementReader() {
    return this.elementReader;
  }

  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public List<D> read(List<D> reuse, Decoder decoder) throws IOException {
    long l = decoder.readArrayStart();
    List<D> array = new GenericData.Array<>( (int)l, getSchema() );
    while (l > 0) {
      for (long i = 0; i < l; i++) {
        array.add(elementReader.read(null, decoder));
      }
      l = decoder.arrayNext();
    }
    return array;
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    long l = decoder.readArrayStart();
    while (l > 0) {
      for (long i = 0; i < l; i++) {
        elementReader.skip(decoder);
      }
      l = decoder.arrayNext();
    }
  }
}
