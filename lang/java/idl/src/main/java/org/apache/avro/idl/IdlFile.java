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
  private final Schema mainSchema;
  private final Protocol protocol;
  private final String namespace;
  private final Map<String, Schema> namedSchemas;
  private final List<String> warnings;

  IdlFile(Protocol protocol, List<String> warnings) {
    this(protocol.getNamespace(), protocol.getTypes(), null, protocol, warnings);
  }

  IdlFile(String namespace, Schema mainSchema, Iterable<Schema> schemas, List<String> warnings) {
    this(namespace, schemas, mainSchema, null, warnings);
  }

  private IdlFile(String namespace, Iterable<Schema> schemas, Schema mainSchema, Protocol protocol,
      List<String> warnings) {
    this.namespace = namespace;
    this.namedSchemas = new LinkedHashMap<>();
    for (Schema namedSchema : schemas) {
      this.namedSchemas.put(namedSchema.getFullName(), namedSchema);
    }
    this.mainSchema = mainSchema;
    this.protocol = protocol;
    this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
  }

  /**
   * The (main) schema defined by the IDL file.
   */
  public Schema getMainSchema() {
    return mainSchema;
  }

  /**
   * The protocol defined by the IDL file.
   */
  public Protocol getProtocol() {
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
   * The default namespace to resolve schema names against.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * The named schemas defined by the IDL file, mapped by their full name.
   */
  public Map<String, Schema> getNamedSchemas() {
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
    Schema result = namedSchemas.get(name);
    if (result != null) {
      return result;
    }
    if (namespace != null && !name.contains(".")) {
      result = namedSchemas.get(namespace + '.' + name);
    }
    return result;
  }

  // Visible for testing
  String outputString() {
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
