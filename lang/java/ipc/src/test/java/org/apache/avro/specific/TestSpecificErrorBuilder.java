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
package org.apache.avro.specific;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.avro.test.errors.TestError;
import org.junit.jupiter.api.Test;

/**
 * Unit test for the SpecificErrorBuilderBase class.
 */
public class TestSpecificErrorBuilder {
  @Test
  void specificErrorBuilder() {
    TestError.Builder testErrorBuilder = TestError.newBuilder().setValue("value").setCause(new NullPointerException())
        .setMessage$("message$");

    // Test has methods
    assertTrue(testErrorBuilder.hasValue());
    assertNotNull(testErrorBuilder.getValue());
    assertTrue(testErrorBuilder.hasCause());
    assertNotNull(testErrorBuilder.getCause());
    assertTrue(testErrorBuilder.hasMessage$());
    assertNotNull(testErrorBuilder.getMessage$());

    TestError testError = testErrorBuilder.build();
    assertEquals("value", testError.getValue());
    assertEquals("value", testError.getMessage());
    assertEquals("message$", testError.getMessage$());

    // Test copy constructor
    assertEquals(testErrorBuilder, TestError.newBuilder(testErrorBuilder));
    assertEquals(testErrorBuilder, TestError.newBuilder(testError));

    TestError error = new TestError("value", new NullPointerException());
    error.setMessage$("message");
    assertEquals(error,
        TestError.newBuilder().setValue("value").setCause(new NullPointerException()).setMessage$("message").build());

    // Test clear
    testErrorBuilder.clearValue();
    assertFalse(testErrorBuilder.hasValue());
    assertNull(testErrorBuilder.getValue());
    testErrorBuilder.clearCause();
    assertFalse(testErrorBuilder.hasCause());
    assertNull(testErrorBuilder.getCause());
    testErrorBuilder.clearMessage$();
    assertFalse(testErrorBuilder.hasMessage$());
    assertNull(testErrorBuilder.getMessage$());
  }

  @Test
  void attemptToSetNonNullableFieldToNull() {
    assertThrows(org.apache.avro.AvroRuntimeException.class, () -> {
      TestError.newBuilder().setMessage$(null);
    });
  }
}
