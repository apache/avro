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
package org.apache.avro.reflect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

public class TestReflectAllowNulls {

  private static class Primitives {
    boolean aBoolean;
    byte aByte;
    short aShort;
    int anInt;
    long aLong;
    float aFloat;
    double aDouble;
  }

  private static class Wrappers {
    Boolean aBoolean;
    Byte aByte;
    Short aShort;
    Integer anInt;
    Long aLong;
    Float aFloat;
    Double aDouble;
    Primitives anObject;
  }

  private static class AllowNullWithNullable {
    @Nullable
    Double aDouble;

    @AvroSchema("[\"double\", \"long\"]")
    Object doubleOrLong;

    @Nullable
    @AvroSchema("[\"double\", \"long\"]")
    Object doubleOrLongOrNull1;

    @AvroSchema("[\"double\", \"long\", \"null\"]")
    Object doubleOrLongOrNull2;

    @Nullable
    @AvroSchema("[\"double\", \"long\", \"null\"]")
    Object doubleOrLongOrNull3;
  }

  @Test
  void primitives() {
    // AllowNull only makes fields nullable, so testing must use a base record
    Schema primitives = ReflectData.AllowNull.get().getSchema(Primitives.class);
    assertEquals(requiredSchema(boolean.class), primitives.getField("aBoolean").schema());
    assertEquals(requiredSchema(byte.class), primitives.getField("aByte").schema());
    assertEquals(requiredSchema(short.class), primitives.getField("aShort").schema());
    assertEquals(requiredSchema(int.class), primitives.getField("anInt").schema());
    assertEquals(requiredSchema(long.class), primitives.getField("aLong").schema());
    assertEquals(requiredSchema(float.class), primitives.getField("aFloat").schema());
    assertEquals(requiredSchema(double.class), primitives.getField("aDouble").schema());
  }

  @Test
  void wrappers() {
    // AllowNull only makes fields nullable, so testing must use a base record
    Schema wrappers = ReflectData.AllowNull.get().getSchema(Wrappers.class);
    assertEquals(nullableSchema(boolean.class), wrappers.getField("aBoolean").schema());
    assertEquals(nullableSchema(byte.class), wrappers.getField("aByte").schema());
    assertEquals(nullableSchema(short.class), wrappers.getField("aShort").schema());
    assertEquals(nullableSchema(int.class), wrappers.getField("anInt").schema());
    assertEquals(nullableSchema(long.class), wrappers.getField("aLong").schema());
    assertEquals(nullableSchema(float.class), wrappers.getField("aFloat").schema());
    assertEquals(nullableSchema(double.class), wrappers.getField("aDouble").schema());
    assertEquals(nullableSchema(Primitives.class), wrappers.getField("anObject").schema());
  }

  @Test
  void allowNullWithNullableAnnotation() {
    Schema withNullable = ReflectData.AllowNull.get().getSchema(AllowNullWithNullable.class);

    assertEquals(nullableSchema(double.class), withNullable.getField("aDouble").schema(),
        "Should produce a nullable double");

    Schema nullableDoubleOrLong = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.LONG)));

    assertEquals(nullableDoubleOrLong, withNullable.getField("doubleOrLong").schema(),
        "Should add null to a non-null union");

    assertEquals(nullableDoubleOrLong, withNullable.getField("doubleOrLongOrNull1").schema(),
        "Should add null to a non-null union");

    Schema doubleOrLongOrNull = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.DOUBLE),
        Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL)));

    assertEquals(doubleOrLongOrNull, withNullable.getField("doubleOrLongOrNull2").schema(),
        "Should add null to a non-null union");

    assertEquals(doubleOrLongOrNull, withNullable.getField("doubleOrLongOrNull3").schema(),
        "Should add null to a non-null union");
  }

  private Schema requiredSchema(Class<?> type) {
    return ReflectData.get().getSchema(type);
  }

  private Schema nullableSchema(Class<?> type) {
    return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), ReflectData.get().getSchema(type)));
  }
}
