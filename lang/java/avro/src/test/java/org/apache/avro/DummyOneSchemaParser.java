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

import java.io.IOException;
import java.net.URI;

public class DummyOneSchemaParser implements FormattedSchemaParser {
  public static final String SCHEMA_TEXT_ONE = "one";
  public static final Schema FIXED_SCHEMA = Schema.createFixed("DummyOne", null, "tests", 42);
  public static final String SCHEMA_TEXT_ERROR = "error";
  public static final String SCHEMA_TEXT_IO_ERROR = "ioerror";
  public static final String ERROR_MESSAGE = "Syntax error";
  public static final String IO_ERROR_MESSAGE = "I/O error";

  @Override
  public Schema parse(NameContext nameContext, URI baseUri, CharSequence formattedSchema)
      throws IOException, SchemaParseException {
    if (SCHEMA_TEXT_ONE.contentEquals(formattedSchema)) {
      return FIXED_SCHEMA;
    } else if (SCHEMA_TEXT_ERROR.contentEquals(formattedSchema)) {
      throw new SchemaParseException(ERROR_MESSAGE);
    } else if (SCHEMA_TEXT_IO_ERROR.contentEquals(formattedSchema)) {
      throw new IOException(IO_ERROR_MESSAGE);
    }
    // Syntax not recognized
    return null;
  }
}
