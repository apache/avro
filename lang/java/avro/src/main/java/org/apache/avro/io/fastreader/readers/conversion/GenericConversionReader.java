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
package org.apache.avro.io.fastreader.readers.conversion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.fastreader.readers.FieldReader;

public class GenericConversionReader<D> implements FieldReader<D> {

  private final FieldReader<?> parentReader;
  private final Conversion<D> conversion;
  private final Schema schema;

  @SuppressWarnings("unchecked")
  public GenericConversionReader(FieldReader<?> parentReader, Conversion<?> conversion,
      Schema schema) {
    this.parentReader = parentReader;
    this.conversion = (Conversion<D>) conversion;
    this.schema = schema;
  }

  @Override
  public D read(D reuse, Decoder decoder) throws IOException {
    final LogicalType logicalType = schema.getLogicalType();
    final Object parentObject = parentReader.read(null, decoder);
    switch (schema.getType()) {
      case ARRAY:
        return conversion.fromArray((Collection<?>) parentObject, schema, logicalType);
      case BOOLEAN:
        return conversion.fromBoolean((Boolean) parentObject, schema, logicalType);
      case BYTES:
        return conversion.fromBytes((ByteBuffer) parentObject, schema, logicalType);
      case DOUBLE:
        return conversion.fromDouble((Double) parentObject, schema, logicalType);
      case ENUM:
        return conversion.fromEnumSymbol((GenericEnumSymbol) parentObject, schema, logicalType);
      case FIXED:
        return conversion.fromFixed((GenericFixed) parentObject, schema, logicalType);
      case FLOAT:
        return conversion.fromFloat((Float) parentObject, schema, logicalType);
      case INT:
        return conversion.fromInt((Integer) parentObject, schema, logicalType);
      case LONG:
        return conversion.fromLong((Long) parentObject, schema, logicalType);
      case MAP:
        return conversion.fromMap((Map<?, ?>) parentObject, schema, logicalType);
      case RECORD:
        return conversion.fromRecord((IndexedRecord) parentObject, schema, logicalType);
      case STRING:
        return conversion.fromCharSequence((CharSequence) parentObject, schema, logicalType);
      case UNION:
      case NULL:
      default:
        throw new IllegalArgumentException("Conversion not possible for type " + schema.getType());
    }
  }

  @Override
  public boolean canReuse() {
    return false;
  }

  @Override
  public void skip(Decoder decoder) throws IOException {
    parentReader.skip(decoder);
  }
}
