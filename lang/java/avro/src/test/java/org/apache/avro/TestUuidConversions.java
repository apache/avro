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

import java.math.BigInteger;
import java.util.UUID;
import java.util.stream.Stream;

public class TestUuidConversions {

  private Conversions.UUIDConversion uuidConversion = new Conversions.UUIDConversion();

  private Schema fixed = Schema.createFixed("fixed", "doc", "", Long.BYTES * 2);
  private Schema fixedUuid = LogicalTypes.uuid().addToSchema(fixed);

  private Schema string = Schema.createFixed("fixed", "doc", "", Long.BYTES * 2);
  private Schema stringUuid = LogicalTypes.uuid().addToSchema(string);

  @ParameterizedTest
  @MethodSource("uuidData")
  void uuidFixed(UUID uuid) {
    GenericFixed value = uuidConversion.toFixed(uuid, fixedUuid, LogicalTypes.uuid());

    byte[] b = new byte[Long.BYTES];
    System.arraycopy(value.bytes(), 0, b, 0, b.length);
    Assertions.assertEquals(uuid.getMostSignificantBits(), new BigInteger(b).longValue());
    System.arraycopy(value.bytes(), Long.BYTES, b, 0, b.length);
    Assertions.assertEquals(uuid.getLeastSignificantBits(), new BigInteger(b).longValue());

    UUID uuid1 = uuidConversion.fromFixed(value, fixedUuid, LogicalTypes.uuid());
    Assertions.assertEquals(uuid, uuid1);
  }

  @ParameterizedTest
  @MethodSource("uuidData")
  void uuidCharSequence(UUID uuid) {
    CharSequence value = uuidConversion.toCharSequence(uuid, stringUuid, LogicalTypes.uuid());

    Assertions.assertEquals(uuid.toString(), value.toString());

    UUID uuid1 = uuidConversion.fromCharSequence(value, stringUuid, LogicalTypes.uuid());
    Assertions.assertEquals(uuid, uuid1);
  }

  public static Stream<Arguments> uuidData() {
    return Stream.of(Arguments.of(new UUID(Long.MIN_VALUE, Long.MAX_VALUE)), Arguments.of(new UUID(-1, 0)),
        Arguments.of(UUID.randomUUID()), Arguments.of(UUID.randomUUID()));
  }

}
