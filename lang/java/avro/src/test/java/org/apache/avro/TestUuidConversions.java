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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

class TestUuidConversions {

  private Conversions.UUIDConversion uuidConversion = new Conversions.UUIDConversion();

  private Schema fixed = Schema.createFixed("fixed", "doc", "", Long.BYTES * 2);
  private Schema fixedUUid = LogicalTypes.uuid().addToSchema(fixed);

  @ParameterizedTest
  @MethodSource("uuidData")
  void uuidFixed(UUID uuid) {
    GenericFixed value = uuidConversion.toFixed(uuid, fixedUUid, LogicalTypes.uuid());
    UUID uuid1 = uuidConversion.fromFixed(value, fixedUUid, LogicalTypes.uuid());
    Assertions.assertEquals(uuid, uuid1);
  }

  @ParameterizedTest
  @MethodSource("uuidData")
  void uuidBytes(UUID uuid) {
    Schema schema = Schema.create(Schema.Type.BYTES);
    Schema bytesUUid = LogicalTypes.uuid().addToSchema(schema);
    ByteBuffer value = uuidConversion.toBytes(uuid, bytesUUid, LogicalTypes.uuid());
    UUID uuid1 = uuidConversion.fromBytes(value, bytesUUid, LogicalTypes.uuid());
    Assertions.assertEquals(uuid, uuid1);
  }

  public static Stream<Arguments> uuidData() {
    return Stream.of(Arguments.of(new UUID(Long.MIN_VALUE, Long.MAX_VALUE)), Arguments.of(new UUID(-1, 0)),
        Arguments.of(UUID.randomUUID()), Arguments.of(UUID.randomUUID()));
  }

}
