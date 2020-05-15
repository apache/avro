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

import org.apache.avro.generic.GenericFixed;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static java.math.RoundingMode.HALF_EVEN;
import static org.junit.Assert.assertEquals;

public class TestDecimalConversion {

  private static final Conversion<BigDecimal> CONVERSION = new Conversions.DecimalConversion();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Schema schema;
  private LogicalType logicalType;

  @Before
  public void setup() {
    schema = Schema.createFixed("aFixed", null, null, 12);
    schema.addProp("logicalType", "decimal");
    schema.addProp("precision", 28);
    schema.addProp("scale", 15);
    logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
  }

  @Test
  public void testToFromBytes() {
    final BigDecimal value = BigDecimal.valueOf(10.99).setScale(15, HALF_EVEN);
    final ByteBuffer byteBuffer = CONVERSION.toBytes(value, schema, logicalType);
    final BigDecimal result = CONVERSION.fromBytes(byteBuffer, schema, logicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToFromBytesMaxPrecision() {
    final BigDecimal value = new BigDecimal("4567335489766.99834").setScale(15, HALF_EVEN);
    final ByteBuffer byteBuffer = CONVERSION.toBytes(value, schema, logicalType);
    final BigDecimal result = CONVERSION.fromBytes(byteBuffer, schema, logicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToBytesPrecisionError() {
    final BigDecimal value = new BigDecimal("1.07046455859736525E+18").setScale(15, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with precision 34 as max precision 28");
    CONVERSION.toBytes(value, schema, logicalType);
  }

  @Test
  public void testToBytesFixedSmallerScale() {
    final BigDecimal value = new BigDecimal("99892.1234").setScale(10, HALF_EVEN);
    final ByteBuffer byteBuffer = CONVERSION.toBytes(value, schema, logicalType);
    final BigDecimal result = CONVERSION.fromBytes(byteBuffer, schema, logicalType);
    assertEquals(new BigDecimal("99892.123400000000000"), result);
  }

  @Test
  public void testToBytesScaleError() {
    final BigDecimal value = new BigDecimal("4567335489766").setScale(16, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with scale 16 as scale 15");
    CONVERSION.toBytes(value, schema, logicalType);
  }

  @Test
  public void testToFromFixed() {
    final BigDecimal value = new BigDecimal("3").setScale(15, HALF_EVEN);
    final GenericFixed fixed = CONVERSION.toFixed(value, schema, logicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, schema, logicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToFromFixedMaxPrecision() {
    final BigDecimal value = new BigDecimal("4567335489766.99834").setScale(15, HALF_EVEN);
    final GenericFixed fixed = CONVERSION.toFixed(value, schema, logicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, schema, logicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToFixedPrecisionError() {
    final BigDecimal value = new BigDecimal("1.07046455859736525E+18").setScale(15, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with precision 34 as max precision 28");
    CONVERSION.toFixed(value, schema, logicalType);
  }

  @Test
  public void testToFromFixedSmallerScale() {
    final BigDecimal value = new BigDecimal("99892.1234").setScale(10, HALF_EVEN);
    final GenericFixed fixed = CONVERSION.toFixed(value, schema, logicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, schema, logicalType);
    assertEquals(new BigDecimal("99892.123400000000000"), result);
  }

  @Test
  public void testToFixedScaleError() {
    final BigDecimal value = new BigDecimal("4567335489766").setScale(16, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with scale 16 as scale 15");
    CONVERSION.toFixed(value, schema, logicalType);
  }

}
