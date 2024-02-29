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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.function.Function;

public interface SchemaJsonSerDe {

  SchemaJsonSerDe DEFAULT = new SchemaJsonSerDe() {
  };

  /**
   * Read Avro schema in a custom way
   * 
   * @param jsonObject the json object that represents the schema
   * @return the read avro schema, if null is returned the avro schema is read via
   *         standard parsing.
   */
  default Schema read(Function<String, JsonNode> jsonObject) {
    return null;
  }

  /**
   * Write avro schema in a custom way.
   * 
   * @param schema - the schema to be written.
   * @param gen    - the json generator to write the schema to.
   * @return true if schema was written to the JsonGenerator, false otherwise
   *         where the schema that is being written in a standard way.
   * @throws IOException
   */
  default boolean write(Schema schema, JsonGenerator gen) throws IOException {
    return false;
  }
}
