/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.logicaltypes;

import java.util.Collection;
import java.util.List;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class AvroArray implements AvroPrimitive {
  public static final String NAME = "ARRAY";
  public static final String TYPENAME = NAME;
  private static final AvroArray ELEMENT = new AvroArray();

  public AvroArray() {
    super();
  }

  public static AvroArray create() {
    return ELEMENT;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public Collection<?> convertToRawType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof List) {
      return (Collection<?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Collection");
  }

  @Override
  public Collection<?> convertToLogicalType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Collection) {
      return (Collection<?>) value;
    }
    throw new AvroTypeException(
        "Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Collection");
  }

  @Override
  public Type getBackingType() {
    return Type.ARRAY;
  }

  @Override
  public Schema getRecommendedSchema() {
    return null;
  }

  @Override
  public AvroType getAvroType() {
    return AvroType.AVROARRAY;
  }

  @Override
  public Class<?> getConvertedType() {
    return Collection.class;
  }
}
