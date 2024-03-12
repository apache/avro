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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class SchemaNameValidatorTest {

  @ParameterizedTest
  @MethodSource("data")
  void validator(NameValidator validator, String input, boolean expectedResult) {
    NameValidator.Result result = validator.validate(input);
    Assertions.assertEquals(expectedResult, result.isOK(), result.getErrors());
  }

  static Stream<Arguments> data() {
    return Stream.of(Arguments.of(NameValidator.UTF_VALIDATOR, null, false), // null not accepted
        Arguments.of(NameValidator.STRICT_VALIDATOR, null, false), // null not accepted
        Arguments.of(NameValidator.UTF_VALIDATOR, "", false), // empty not accepted
        Arguments.of(NameValidator.STRICT_VALIDATOR, "", false), // empty not accepted
        Arguments.of(NameValidator.UTF_VALIDATOR, "Hello world", false), // space not accepted
        Arguments.of(NameValidator.STRICT_VALIDATOR, "Hello world", false), // space not accepted
        Arguments.of(NameValidator.UTF_VALIDATOR, "H&", false), // non letter or digit not accepted
        Arguments.of(NameValidator.STRICT_VALIDATOR, "H&", false), // non letter or digit not accepted
        Arguments.of(NameValidator.UTF_VALIDATOR, "H=", false), // non letter or digit not accepted
        Arguments.of(NameValidator.STRICT_VALIDATOR, "H=", false), // non letter or digit not accepted
        Arguments.of(NameValidator.UTF_VALIDATOR, "H]", false), // non letter or digit not accepted
        Arguments.of(NameValidator.STRICT_VALIDATOR, "H]", false), // non letter or digit not accepted
        Arguments.of(NameValidator.UTF_VALIDATOR, "Hello_world", true),
        Arguments.of(NameValidator.STRICT_VALIDATOR, "Hello_world", true),
        Arguments.of(NameValidator.UTF_VALIDATOR, "éàçô", true), // Accept accent
        Arguments.of(NameValidator.STRICT_VALIDATOR, "éàçô", false), // Not Accept accent
        Arguments.of(NameValidator.UTF_VALIDATOR, "5éàçô", false), // can't start with number
        Arguments.of(NameValidator.STRICT_VALIDATOR, "5éàçô", false), // can't start with number
        Arguments.of(NameValidator.UTF_VALIDATOR, "_Hello_world", true),
        Arguments.of(NameValidator.STRICT_VALIDATOR, "_Hello_world", true));
  }

}
