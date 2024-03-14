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

import org.apache.avro.ParseContext;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A parsed IdlFile. Provides access to the named schemas in the IDL file and
 * the protocol containing the schemas.
 */
public class IdlFile {
  private final Object resolveLock = new Object();
  private volatile ParseContext parseContext;
  private Schema mainSchema;
  private Protocol protocol;
  private Map<String, Schema> namedSchemas;
  private final List<String> warnings;

  IdlFile(Protocol protocol, ParseContext context, List<String> warnings) {
    this(context, null, protocol, warnings);
  }

  IdlFile(Schema mainSchema, ParseContext context, List<String> warnings) {
    this(context, mainSchema, null, warnings);
  }

  private IdlFile(ParseContext context, Schema mainSchema, Protocol protocol, List<String> warnings) {
    this.parseContext = context;
    this.namedSchemas = new LinkedHashMap<>();
    this.mainSchema = mainSchema;
    this.protocol = protocol;
    this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
  }

  /**
   * The (main) schema defined by the IDL file.
   */
  public Schema getMainSchema() {
    if (mainSchema == null) {
      return null;
    }
    ensureSchemasAreResolved();
    return mainSchema;
  }

  private void ensureSchemasAreResolved() {
    if (parseContext != null) {
      synchronized (resolveLock) {
        if (parseContext != null) {
          parseContext.commit();
          List<Schema> schemas = parseContext.resolveAllSchemas();
          schemas.forEach(schema -> namedSchemas.put(schema.getFullName(), schema));
          if (mainSchema != null) {
            mainSchema = parseContext.resolve(mainSchema);
          }
          if (protocol != null) {
            protocol.setTypes(schemas);
            Map<String, Protocol.Message> messages = protocol.getMessages();
            for (Map.Entry<String, Protocol.Message> entry : messages.entrySet()) {
              Protocol.Message oldValue = entry.getValue();
              Protocol.Message newValue;
              if (oldValue.isOneWay()) {
                newValue = protocol.createMessage(oldValue.getName(), oldValue.getDoc(), oldValue,
                    parseContext.resolve(oldValue.getRequest()));
              } else {
                Schema request = parseContext.resolve(oldValue.getRequest());
                Schema response = parseContext.resolve(oldValue.getResponse());
                Schema errors = parseContext.resolve(oldValue.getErrors());
                newValue = protocol.createMessage(oldValue.getName(), oldValue.getDoc(), oldValue, request, response,
                    errors);
              }
              entry.setValue(newValue);
            }
          }
        }
      }
    }
  }

  /**
   * The protocol defined by the IDL file.
   */
  public Protocol getProtocol() {
    if (protocol == null) {
      return null;
    }
    ensureSchemasAreResolved();
    return protocol;
  }

  public List<String> getWarnings() {
    return warnings;
  }

  public List<String> getWarnings(String importFile) {
    return warnings.stream()
        .map(warning -> importFile + ' ' + Character.toLowerCase(warning.charAt(0)) + warning.substring(1))
        .collect(Collectors.toList());
  }

  /**
   * The named schemas defined by the IDL file, mapped by their full name.
   */
  public Map<String, Schema> getNamedSchemas() {
    ensureSchemasAreResolved();
    return Collections.unmodifiableMap(namedSchemas);
  }

  /**
   * Get a named schema defined by the IDL file, by name. The name can be a simple
   * name in the default namespace of the IDL file (e.g., the namespace of the
   * protocol), or a full name.
   *
   * @param name the full name of the schema, or a simple name
   * @return the schema, or {@code null} if it does not exist
   */
  public Schema getNamedSchema(String name) {
    ensureSchemasAreResolved();
    return namedSchemas.get(name);
  }

  // Visible for testing
  String outputString() {
    ensureSchemasAreResolved();
    if (protocol != null) {
      return protocol.toString();
    }
    if (mainSchema != null) {
      return mainSchema.toString();
    }
    if (namedSchemas.isEmpty()) {
      return "[]";
    } else {
      StringBuilder buffer = new StringBuilder();
      for (Schema schema : namedSchemas.values()) {
        buffer.append(',').append(schema);
      }
      buffer.append(']').setCharAt(0, '[');
      return buffer.toString();
    }
  }
}
