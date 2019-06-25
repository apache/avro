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

package org.apache.avro.codegentest;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class CustomDecimalConversion extends Conversion<CustomDecimal> {
  @Override
  public Class<CustomDecimal> getConvertedType() {
    return CustomDecimal.class;
  }

  @Override
  public String getLogicalTypeName() {
    return "decimal";
  }

  @Override
  public CustomDecimal fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
    int scale = ((LogicalTypes.Decimal) type).getScale();
    byte[] bytes = value.get(new byte[value.remaining()]).array();
    return new CustomDecimal(new BigInteger(bytes), scale);
  }

  @Override
  public ByteBuffer toBytes(CustomDecimal value, Schema schema, LogicalType type) {
    int scale = ((LogicalTypes.Decimal) type).getScale();
    return ByteBuffer.wrap(value.toByteArray(scale));
  }
}
