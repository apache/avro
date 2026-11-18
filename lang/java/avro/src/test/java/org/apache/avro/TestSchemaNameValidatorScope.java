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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.JsonParserDelegate;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scope (restoration) contract of {@link Schema#VALIDATE_NAMES}. The parser
 * installs its own validator while parsing and must restore the pre-existing
 * validator afterwards, including on reentrant and exceptional paths.
 */
class TestSchemaNameValidatorScope {

  private static final String VALID_RECORD_JSON = "{\"type\":\"record\",\"name\":\"ValidName\",\"fields\":[]}";
  private static final String INVALID_NAME_RECORD_JSON = "{\"type\":\"record\",\"name\":\"not valid\",\"fields\":[]}";
  private static final String MALFORMED_JSON = "{this is not json";

  private static final Field VALIDATE_NAMES_FIELD = validateNamesField();
  private static final Method PARSER_PARSE_METHOD = parserParseMethod();

  private NameValidator priorValidator;

  private static Field validateNamesField() {
    try {
      Field field = Schema.class.getDeclaredField("VALIDATE_NAMES");
      field.setAccessible(true);
      return field;
    } catch (NoSuchFieldException e) {
      throw new AssertionError("Schema.VALIDATE_NAMES not found", e);
    }
  }

  private static Method parserParseMethod() {
    try {
      Method method = Schema.Parser.class.getDeclaredMethod("parse", JsonParser.class, boolean.class, boolean.class);
      method.setAccessible(true);
      return method;
    } catch (NoSuchMethodException e) {
      throw new AssertionError("Schema.Parser.parse(JsonParser, boolean, boolean) not found", e);
    }
  }

  @BeforeEach
  void saveAndClearValidatorScope() {
    ThreadLocal<NameValidator> validateNames = validateNames();
    priorValidator = validateNames.get();
    validateNames.remove();
  }

  @AfterEach
  void restoreValidatorScope() {
    ThreadLocal<NameValidator> validateNames = validateNames();
    if (priorValidator == null) {
      validateNames.remove();
    } else {
      validateNames.set(priorValidator);
    }
  }

  @SuppressWarnings("unchecked")
  private static ThreadLocal<NameValidator> validateNames() {
    try {
      return (ThreadLocal<NameValidator>) VALIDATE_NAMES_FIELD.get(null);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Schema.VALIDATE_NAMES is not accessible", e);
    }
  }

  private static Schema invokeParserParse(Schema.Parser parser, JsonParser jsonParser, boolean allowDanglingContent,
      boolean resolveSchema) throws Throwable {
    try {
      return (Schema) PARSER_PARSE_METHOD.invoke(parser, jsonParser, allowDanglingContent, resolveSchema);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static NameValidator newPermissiveValidator() {
    return new NameValidator() {
      @Override
      public Result validate(String name) {
        return NameValidator.OK;
      }
    };
  }

  private static CloseFailingJsonParser closeFailingParser(String json) throws IOException {
    return new CloseFailingJsonParser(Schema.FACTORY.createParser(json));
  }

  @Test
  void defaultRecordConstructionDoesNotRetainValidator() {
    Schema.createRecord("Record", null, null, false);
    assertNull(validateNames().get());
  }

  @Test
  void defaultValidatorAcceptsValidName() {
    Schema schema = Schema.createRecord("ValidName", null, null, false);
    assertEquals("ValidName", schema.getName());
  }

  @Test
  void defaultValidatorRejectsInvalidName() {
    assertThrows(SchemaParseException.class, () -> Schema.createRecord("not valid", null, null, false));
  }

  @Test
  void defaultParserRestoresValidatorScopeAfterSuccessfulParse() {
    new Schema.Parser().parse(VALID_RECORD_JSON);
    assertNull(validateNames().get());
  }

  @Test
  void defaultParserRestoresValidatorScopeAfterInvalidSchemaName() {
    assertThrows(SchemaParseException.class, () -> new Schema.Parser().parse(INVALID_NAME_RECORD_JSON));
    assertNull(validateNames().get());
  }

  @Test
  void defaultParserRestoresValidatorScopeAfterMalformedJson() {
    assertThrows(SchemaParseException.class, () -> new Schema.Parser().parse(MALFORMED_JSON));
    assertNull(validateNames().get());
  }

  @Test
  void defaultParserPreservesPreExistingCustomValidatorScope() {
    NameValidator custom = newPermissiveValidator();
    validateNames().set(custom);
    new Schema.Parser(NameValidator.UTF_VALIDATOR).parse(VALID_RECORD_JSON);
    assertSame(custom, validateNames().get());
  }

  @Test
  void reentrantValidatorInnerParseSuccessRestoresOuterValidatorScope() {
    AtomicBoolean invoked = new AtomicBoolean();
    NameValidator[] holder = new NameValidator[1];
    holder[0] = reentrantValidator(invoked, holder, VALID_RECORD_JSON);

    String outerRecordJson = "{\"type\":\"record\",\"name\":\"OuterRecord\",\"fields\":[{\"name\":\"OuterField\",\"type\":\"string\"}]}";
    new Schema.Parser(holder[0]).parse(outerRecordJson);

    assertTrue(invoked.get());
    assertNull(validateNames().get());
  }

  @Test
  void reentrantValidatorInnerParseFailureRestoresOuterValidatorScope() {
    AtomicBoolean invoked = new AtomicBoolean();
    NameValidator[] holder = new NameValidator[1];
    holder[0] = reentrantValidator(invoked, holder, INVALID_NAME_RECORD_JSON);

    String outerRecordJson = "{\"type\":\"record\",\"name\":\"OuterRecord\",\"fields\":[{\"name\":\"OuterField\",\"type\":\"string\"}]}";
    assertThrows(SchemaParseException.class, () -> new Schema.Parser(holder[0]).parse(outerRecordJson));

    assertTrue(invoked.get());
    assertNull(validateNames().get());
  }

  @Test
  void subsequentDefaultParserUsesUtfValidation() {
    new Schema.Parser(NameValidator.NO_VALIDATION).parse(INVALID_NAME_RECORD_JSON);
    assertNull(validateNames().get());
    Schema.Parser defaultParser = new Schema.Parser();
    defaultParser.parse(VALID_RECORD_JSON);
    assertThrows(SchemaParseException.class, () -> defaultParser.parse(INVALID_NAME_RECORD_JSON));
    assertNull(validateNames().get());
  }

  @Test
  void closeFailureRestoresUnsetValidatorScope() throws Throwable {
    NameValidator parserValidator = newPermissiveValidator();
    Schema.Parser parser = new Schema.Parser(parserValidator);
    CloseFailingJsonParser failing = closeFailingParser(VALID_RECORD_JSON);

    IOException fault = assertThrows(IOException.class, () -> invokeParserParse(parser, failing, false, true));

    assertEquals("close fault", fault.getMessage());
    assertTrue(failing.closeInvoked());
    assertNull(validateNames().get());
  }

  @Test
  void closeFailureRestoresDistinctPriorValidatorScope() throws Throwable {
    NameValidator prior = newPermissiveValidator();
    NameValidator parserValidator = newPermissiveValidator();
    validateNames().set(prior);
    Schema.Parser parser = new Schema.Parser(parserValidator);
    CloseFailingJsonParser failing = closeFailingParser(VALID_RECORD_JSON);

    IOException fault = assertThrows(IOException.class, () -> invokeParserParse(parser, failing, false, true));

    assertEquals("close fault", fault.getMessage());
    assertTrue(failing.closeInvoked());
    assertSame(prior, validateNames().get());
  }

  /**
   * A validator that runs an inner schema parse from inside
   * {@link NameValidator#validate(String)}. It asserts that the outer validator
   * is still installed both before and after the inner parse, and guards against
   * being entered more than once.
   */
  private static NameValidator reentrantValidator(AtomicBoolean invoked, NameValidator[] holder, String innerJson) {
    return new NameValidator() {
      @Override
      public Result validate(String name) {
        if ("OuterField".equals(name) && invoked.compareAndSet(false, true)) {
          assertSame(holder[0], validateNames().get());
          try {
            new Schema.Parser(NameValidator.UTF_VALIDATOR).parse(innerJson);
          } finally {
            assertSame(holder[0], validateNames().get());
          }
          return NameValidator.OK;
        }
        return NameValidator.UTF_VALIDATOR.validate(name);
      }
    };
  }

  /**
   * A {@link JsonParser} that delegates every call to a real parser but throws an
   * {@link IOException} from {@link #close()}, so the private
   * {@code Schema.Parser.parse(JsonParser, boolean, boolean)} cleanup path can be
   * exercised exactly the way a failing underlying stream close would behave.
   */
  private static final class CloseFailingJsonParser extends JsonParserDelegate {
    private final AtomicBoolean closed = new AtomicBoolean();

    CloseFailingJsonParser(JsonParser delegate) {
      super(delegate);
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      super.close();
      throw new IOException("close fault");
    }

    boolean closeInvoked() {
      return closed.get();
    }
  }
}
