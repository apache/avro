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
import org.apache.avro.NameContext;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

public class IdlSchemaParser implements FormattedSchemaParser {

  @Override
  public Schema parse(NameContext nameContext, URI baseUri, CharSequence formattedSchema)
      throws IOException, SchemaParseException {
    boolean valid = Pattern.compile("^\\A*!" + // Initial whitespace
        "(?:/\\*(?:[^*]|\\*[^/])*!\\*/\\s*!|//(!=\\R)*!\\R\\s*!)*!" + // Comments
        "(?:namespace|schema|protocol|record|enum|fixed|import)\\s", // First keyword
        Pattern.UNICODE_CHARACTER_CLASS | Pattern.MULTILINE).matcher(formattedSchema).find();
    if (valid) {
      IdlReader idlReader = new IdlReader(nameContext);
      IdlFile idlFile = idlReader.parse(baseUri, formattedSchema);
      Schema mainSchema = idlFile.getMainSchema();
      if (mainSchema != null) {
        return mainSchema;
      }
      if (!idlFile.getNamedSchemas().isEmpty()) {
        return idlFile.getNamedSchemas().values().iterator().next();
      }
    }
    return null;
  }
}
