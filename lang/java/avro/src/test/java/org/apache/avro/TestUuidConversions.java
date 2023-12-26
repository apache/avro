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
    ByteBuffer value = uuidConversion.toBytes(uuid, fixedUUid, LogicalTypes.uuid());
    UUID uuid1 = uuidConversion.fromBytes(value, fixedUUid, LogicalTypes.uuid());
    Assertions.assertEquals(uuid, uuid1);
  }

  public static Stream<Arguments> uuidData() {
    return Stream.of(Arguments.of(new UUID(Long.MIN_VALUE, Long.MAX_VALUE)), Arguments.of(new UUID(-1, 0)),
        Arguments.of(UUID.randomUUID()), Arguments.of(UUID.randomUUID()));
  }

}
