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

package org.apache.avro;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import static org.junit.Assert.assertEquals;

public class TestBigDecimalConversions {

  public static final LogicalTypes.Decimal DECIMAL_SCALE_3 = LogicalTypes.decimal(MathContext.DECIMAL32.getPrecision(),
      3);
  public static final LogicalTypes.Decimal DECIMAL_SCALE_5 = LogicalTypes.decimal(MathContext.DECIMAL32.getPrecision(),
      5);
  public static final LogicalTypes.Decimal DECIMAL_SCALE_7 = LogicalTypes.decimal(MathContext.DECIMAL32.getPrecision(),
      7);

  @Test
  public void normalConversion() {
    BigDecimal original = new BigDecimal("123.321");

    assertEquals(original, convertBigDecimalToAndFromCharSequence(DECIMAL_SCALE_3, original));
  }

  @Test(expected = AvroTypeException.class)
  public void conversionWithWrongScale() {
    BigDecimal original = new BigDecimal("42.55555");

    convertBigDecimalToAndFromCharSequence(DECIMAL_SCALE_3, original);
  }

  @Test
  public void conversionToBiggerScale() {
    BigDecimal original = new BigDecimal("1.54321");

    BigDecimal resultWithExtraZeros = convertBigDecimalToAndFromCharSequence(DECIMAL_SCALE_5, DECIMAL_SCALE_7,
        original);
    assertEquals(original.setScale(DECIMAL_SCALE_7.getScale(), RoundingMode.UNNECESSARY), resultWithExtraZeros);
  }

  @Test(expected = ArithmeticException.class)
  public void conversionToSmallerScale() {
    BigDecimal original = new BigDecimal("1.54321");

    // will throw an exception when tries to convert scale 5 to scale 3
    convertBigDecimalToAndFromCharSequence(DECIMAL_SCALE_5, DECIMAL_SCALE_3, original);
  }

  private BigDecimal convertBigDecimalToAndFromCharSequence(LogicalType logicalType, BigDecimal original) {
    return convertBigDecimalToAndFromCharSequence(logicalType, logicalType, original);
  }

  private BigDecimal convertBigDecimalToAndFromCharSequence(LogicalType logicalTypeTo, LogicalType logicalTypeFrom,
      BigDecimal original) {
    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
    Schema schema = Schema.create(Schema.Type.STRING);
    schema.setLogicalType(logicalTypeTo);
    return conversion.fromCharSequence(conversion.toCharSequence(original, schema, logicalTypeTo), schema,
        logicalTypeFrom);
  }
}
