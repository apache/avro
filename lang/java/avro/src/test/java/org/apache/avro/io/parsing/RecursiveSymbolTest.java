/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io.parsing;


import java.util.Arrays;

import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.junit.Test;

/**
 * Unit test to verify that recursive schemas can be validated.
 * See AVRO-2122.
 */
public class RecursiveSymbolTest {

  public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Node\",\"namespace\":\"avro\",\"fields\":[{\"name\":\"value\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");

  @Test
  public void testSomeMethod() throws SchemaValidationException {
    // before AVRO-2122, this would cause a StackOverflowError
	final SchemaValidator backwardValidator = new SchemaValidatorBuilder().canReadStrategy().validateLatest();
	backwardValidator.validate(SCHEMA, Arrays.asList(SCHEMA));
  }
  
}
