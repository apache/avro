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
package org.apache.avro.idl;

import org.apache.avro.FormattedSchemaParser;
import org.apache.avro.ParseContext;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

public class IdlSchemaParser implements FormattedSchemaParser {
  /**
   * Pattern to check if the input can be IDL. It matches initial whitespace and
   * comments, plus all possible starting keywords. The match on the start of the
   * input as well as the use of possessive quantifiers is deliberate: it should
   * fail as fast as possible, as it is assumed that most schemata will not be in
   * IDL format.
   */
  private static final Pattern START_OF_IDL_PATTERN = Pattern.compile("\\A" + // Start of input
      "(?:\\s*+|/\\*(?:[^*]|\\*(?!/))*+\\*/|//(?:(?!\\R).)*+\\R)*+" + // Initial whitespace & comments
      "(?:@|(?:namespace|schema|protocol|record|enum|fixed|import)\\s)", // First keyword or @
      Pattern.UNICODE_CHARACTER_CLASS | Pattern.MULTILINE);

  @Override
  public Schema parse(ParseContext parseContext, URI baseUri, CharSequence formattedSchema)
      throws IOException, SchemaParseException {
    boolean inputCanBeIdl = START_OF_IDL_PATTERN.matcher(formattedSchema).find();
    if (inputCanBeIdl) {
      IdlReader idlReader = new IdlReader(parseContext);
      IdlFile idlFile = idlReader.parse(baseUri, formattedSchema);
      return idlFile.getMainSchema();
    }
    return null;
  }
}
