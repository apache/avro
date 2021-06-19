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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import static java.math.RoundingMode.HALF_EVEN;
import static org.junit.Assert.*;

/*
 *  TODO test for negative duration
 *  TODO test for empty byte array (0 duration)
 */
public class TestDurationConversion {

  private static final Conversion<Duration> CONVERSION = new Conversions.DurationConversion();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Schema schema;
  private LogicalType logicalType;

  @Before
  public void setup() {
    schema = Schema.createFixed("aFixed", null, null, 12);
    schema.addProp("logicalType", "duration");
    logicalType = LogicalTypes.fromSchema(schema);
  }

  @Test
  public void testToFromFixed() {
    final Duration value = Duration.ofDays(10).plusMillis(100);
    final GenericFixed fixed = CONVERSION.toFixed(value, schema, logicalType);
    final Duration result = CONVERSION.fromFixed(fixed, schema, logicalType);
    assertEquals(value, result);
  }

  @Test
  public void testConvertingMillisecondsFromBytes() {
    LogicalTypes.Duration duration = (LogicalTypes.Duration) logicalType;

    final Duration durationValue = Duration.ofMillis(200);

    final byte[] bytes = new byte[]{
      0x0, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0,
      -0x38, 0x0, 0x0, 0x0
    };

    final GenericFixed dd = new GenericData.Fixed(schema, bytes);

    final Duration fromBytes = CONVERSION.fromFixed(dd, schema, duration);

    assertEquals(durationValue, fromBytes);
  }

  @Test
  public void testConvertingDaysFromBytes() {
    LogicalTypes.Duration duration = (LogicalTypes.Duration) logicalType;

    final Duration durationValue = Duration.ofDays(29);

    final byte[] bytes = new byte[]{
      0x0, 0x0, 0x0, 0x0,
      0x1D, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0
    };

    final GenericFixed dd = new GenericData.Fixed(schema, bytes);

    final Duration fromBytes = CONVERSION.fromFixed(dd, schema, duration);

    assertEquals(durationValue, fromBytes);
  }

  @Test
  public void testConvertingMonthsFromBytes() {
    LogicalTypes.Duration duration = (LogicalTypes.Duration) logicalType;

    final Duration durationValue = Duration.ofDays(60);

    final byte[] bytes = new byte[]{
      0x02, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0
    };

    final GenericFixed dd = new GenericData.Fixed(schema, bytes);

    final Duration fromBytes = CONVERSION.fromFixed(dd, schema, duration);

    assertEquals(durationValue, fromBytes);
  }

  @Test
  public void testConvertingMillisecondsToBytes() {
    LogicalTypes.Duration duration = (LogicalTypes.Duration) logicalType;

    final Duration durationValue = Duration.ofMillis(200);

    final byte[] bytes = new byte[]{
      0x0, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0,
      -0x38, 0x0, 0x0, 0x0
    };

    final GenericFixed fixed = CONVERSION.toFixed(durationValue, schema, duration);

    assertArrayEquals(bytes, fixed.bytes());
  }

  @Test
  public void testConvertingMillisecondsWithNanosecondAdjustmentToBytes() {
    LogicalTypes.Duration duration = (LogicalTypes.Duration) logicalType;

    final Duration durationValue = Duration.ofMillis(200).plusNanos(10);

    final byte[] bytes = new byte[]{
      0x0, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0,
      -0x37, 0x0, 0x0, 0x0
    };

    final GenericFixed fixed = CONVERSION.toFixed(durationValue, schema, duration);

    assertArrayEquals(bytes, fixed.bytes());
  }

  @Test
  public void testConvertingDaysToBytes() {
    LogicalTypes.Duration duration = (LogicalTypes.Duration) logicalType;

    final Duration durationValue = Duration.ofDays(29);

    final byte[] bytes = new byte[]{
      0x0, 0x0, 0x0, 0x0,
      0x1D, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0
    };

    final GenericFixed fixed = CONVERSION.toFixed(durationValue, schema, duration);

    assertArrayEquals(bytes, fixed.bytes());
  }

  @Test
  public void testConvertingWeeksToBytes() {
    LogicalTypes.Duration duration = (LogicalTypes.Duration) logicalType;

    final Duration durationValue = Duration.ofDays(60);

    final byte[] bytes = new byte[]{
      0x02, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0,
      0x0, 0x0, 0x0, 0x0
    };

    final GenericFixed fixed = CONVERSION.toFixed(durationValue, schema, duration);

    assertArrayEquals(bytes, fixed.bytes());
  }
}
