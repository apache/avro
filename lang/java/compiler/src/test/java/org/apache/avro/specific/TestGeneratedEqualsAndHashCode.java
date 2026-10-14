/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.specific;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.specific.test.EqualsLine;
import org.apache.avro.specific.test.EqualsNamed;
import org.apache.avro.specific.test.EqualsOrder;
import org.junit.jupiter.api.Test;

/**
 * AVRO-4354: the generated {@code equals()} must not depend on the runtime
 * types of collections and strings, and the generated {@code hashCode()} must
 * be consistent with it.
 */
public class TestGeneratedEqualsAndHashCode {

  private static final UUID ID = UUID.fromString("de5714a7-0000-0000-0000-000000000001");

  private static EqualsOrder roundTrip(EqualsOrder order) throws IOException {
    return EqualsOrder.getDecoder().decode(EqualsOrder.getEncoder().encode(order));
  }

  private static EqualsNamed roundTrip(EqualsNamed named) throws IOException {
    return EqualsNamed.getDecoder().decode(EqualsNamed.getEncoder().encode(named));
  }

  private static void assertEqualBothWays(Object before, Object after) {
    assertEquals(before, after);
    assertEquals(after, before);
    assertEquals(before.hashCode(), after.hashCode());
  }

  @Test
  void arrayOfRecordsWithNullableLogicalType() throws IOException {
    EqualsOrder before = EqualsOrder.newBuilder().setLines(List.of(EqualsLine.newBuilder().setLineId(ID).build()))
        .build();
    EqualsOrder after = roundTrip(before);

    assertEqualBothWays(before, after);
    assertEqualBothWays(after, roundTrip(before));
  }

  @Test
  void arrayOfNullableLogicalType() throws IOException {
    EqualsOrder before = EqualsOrder.newBuilder().setDates(List.of(LocalDate.of(2026, 9, 28))).build();
    EqualsOrder after = roundTrip(before);

    assertEqualBothWays(before, after);
  }

  @Test
  void arrayOfStrings() throws IOException {
    EqualsOrder before = EqualsOrder.newBuilder().setLabels(List.of("a", "b")).build();
    EqualsOrder after = roundTrip(before);

    assertEqualBothWays(before, after);
    assertNotEquals(before, EqualsOrder.newBuilder().setLabels(List.of("a")).build());
  }

  @Test
  void nullableArrayOfStrings() throws IOException {
    EqualsOrder before = EqualsOrder.newBuilder().setOptionalLabels(List.of("a")).build();
    EqualsOrder after = roundTrip(before);

    assertEqualBothWays(before, after);
    assertNotEquals(before, EqualsOrder.newBuilder().build());
  }

  @Test
  void mapOfStrings() throws IOException {
    EqualsOrder before = EqualsOrder.newBuilder().setAttributes(Map.of("k", "v")).build();
    EqualsOrder after = roundTrip(before);

    assertEqualBothWays(before, after);
    assertNotEquals(before, EqualsOrder.newBuilder().setAttributes(Map.of("k", "w")).build());
  }

  @Test
  void hashCodeIsConsistentWithEqualsForCharSequenceFields() throws IOException {
    // Holds java.lang.String values.
    EqualsNamed before = EqualsNamed.newBuilder().setName("a").setNickname("b").build();
    // Holds org.apache.avro.util.Utf8 values.
    EqualsNamed after = roundTrip(before);

    assertEqualBothWays(before, after);

    Set<EqualsNamed> set = new HashSet<>();
    set.add(before);
    assertTrue(set.contains(after));
  }

  @Test
  void hashCodeOfGeneratedRecordMatchesSpecificData() throws IOException {
    EqualsNamed named = EqualsNamed.newBuilder().setName("a").setNickname("b").build();
    assertEquals(SpecificData.get().hashCode(named, named.getSchema()), named.hashCode());
    assertEquals(named.hashCode(), roundTrip(named).hashCode());
  }
}
