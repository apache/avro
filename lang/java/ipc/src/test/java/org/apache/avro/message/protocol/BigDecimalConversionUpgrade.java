/**
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
package org.apache.avro.message.protocol;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

public class BigDecimalConversionUpgrade extends Conversions.DecimalConversion {
  public static final MathContext DECIMAL_CONTEXT = new MathContext(16, RoundingMode.HALF_UP);
  private static final int ROUNDING_SCALE = 5;

  @Override
  public ByteBuffer toBytes(BigDecimal value, Schema schema, LogicalType type) {
    return super.toBytes(round(value), schema, type);
  }

  public static BigDecimal round(BigDecimal value) {
    return value == null ? null : value.setScale(ROUNDING_SCALE, RoundingMode.HALF_UP);
  }

  @Override
  public Schema getRecommendedSchema() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    LogicalTypes.Decimal decimalType = LogicalTypes.decimal(DECIMAL_CONTEXT.getPrecision(), ROUNDING_SCALE);
    decimalType.addToSchema(schema);
    return schema;
  }
}
