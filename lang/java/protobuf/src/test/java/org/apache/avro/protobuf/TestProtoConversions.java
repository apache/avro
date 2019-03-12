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

package org.apache.avro.protobuf;

import com.google.protobuf.Timestamp;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtoConversions.*;
import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;

public class TestProtoConversions {

  private static Schema TIMESTAMP_MILLIS_SCHEMA;
  private static Schema TIMESTAMP_MICROS_SCHEMA;

  @BeforeClass
  public static void createSchemas() {
    TestProtoConversions.TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    TestProtoConversions.TIMESTAMP_MICROS_SCHEMA = LogicalTypes.timestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
  }

  @Test
  public void testTimestampMillisConversion() throws Exception {
    TimestampConversion conversion = new TimestampConversion();
    long nowInstant = new Date().getTime();

    Timestamp now = conversion.fromLong(
        nowInstant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    long roundTrip = conversion.toLong(
        now, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    Assert.assertEquals("Round-trip conversion should work",
        nowInstant, roundTrip);

    long May_28_2015_21_46_53_221_instant = 1432849613221L;
    Timestamp May_28_2015_21_46_53_221 = Timestamp.newBuilder()
      .setSeconds(1432849613L)
      .setNanos(221000000)
      .build();

    Assert.assertEquals("Known timestamp should be correct",
        May_28_2015_21_46_53_221,
        conversion.fromLong(May_28_2015_21_46_53_221_instant,
            TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
    Assert.assertEquals("Known timestamp should be correct",
        May_28_2015_21_46_53_221_instant,
        (long) conversion.toLong(May_28_2015_21_46_53_221,
            TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
  }

  @Test
  public void testTimestampMicrosConversion() throws Exception {
    TimestampMicrosConversion conversion = new TimestampMicrosConversion();

    long May_28_2015_21_46_53_221_843_instant = 1432849613221L * 1000 + 843;
    Timestamp May_28_2015_21_46_53_221_843_ts = Timestamp.newBuilder()
      .setSeconds(1432849613L)
      .setNanos(221843000)
      .build();

    Assert.assertEquals("Known timestamp should be correct",
        May_28_2015_21_46_53_221_843_ts,
        conversion.fromLong(May_28_2015_21_46_53_221_843_instant,
            TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));

    try {
      conversion.toLong(May_28_2015_21_46_53_221_843_ts,
        TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
      Assert.fail("Should not convert DateTime to long");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  /*
  model.addLogicalTypeConversion(new ProtoConversions.TimeMicrosConversion());
  model.addLogicalTypeConversion(new ProtoConversions.TimestampMicrosConversion());
 */
  @Test
  public void testDynamicSchemaWithDateTimeConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("com.google.protobuf.Timestamp", new TimestampConversion());
    Assert.assertEquals("Reflected schema should be logicalType timestampMillis", TIMESTAMP_MILLIS_SCHEMA, schema);
  }

  @Test
  public void testDynamicSchemaWithDateTimeMicrosConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("com.google.protobuf.Timestamp", new TimestampMicrosConversion());
    Assert.assertEquals("Reflected schema should be logicalType timestampMicros", TIMESTAMP_MICROS_SCHEMA, schema);
  }

  private Schema getReflectedSchemaByName(String className, Conversion<?> conversion) throws ClassNotFoundException {
    // one argument: a fully qualified class name
    Class<?> cls = Class.forName(className);

    // get the reflected schema for the given class
    ReflectData model = new ReflectData();
    model.addLogicalTypeConversion(conversion);
    return model.getSchema(cls);
  }

}
