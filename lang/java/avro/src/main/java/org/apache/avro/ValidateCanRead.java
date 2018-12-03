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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro;

/**
 * A {@link SchemaValidationStrategy} that checks that the {@link Schema} to
 * validate can read the existing schema according to the default Avro schema
 * resolution rules.
 *
 */
class ValidateCanRead implements SchemaValidationStrategy {

  /**
   * Validate that the first schema provided can be used to read data written
   * with the second schema, according to the default Avro schema resolution
   * rules.
   *
   * @throws SchemaValidationException
   *           if the first schema cannot read data written by the second.
   */
  @Override
  public void validate(Schema toValidate, Schema existing)
      throws SchemaValidationException {
    ValidateMutualRead.canRead(existing, toValidate);
  }

}
