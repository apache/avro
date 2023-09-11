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

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility class to resolve schemas that are unavailable at the point they are
 * referenced in the IDL.
 */
final class SchemaResolver {

  private SchemaResolver() {
  }

  private static final String UR_SCHEMA_ATTR = "org.apache.avro.idl.unresolved.name";

  private static final String UR_SCHEMA_NAME = "UnresolvedSchema";

  private static final String UR_SCHEMA_NS = "org.apache.avro.compiler";

  /**
   * Create a schema to represent an "unresolved" schema. (used to represent a
   * schema whose definition does not exist, yet).
   *
   * @param name a schema name
   * @return an unresolved schema for the given name
   */
  static Schema unresolvedSchema(final String name) {
    Schema schema = Schema.createRecord(UR_SCHEMA_NAME, "unresolved schema", UR_SCHEMA_NS, false,
        Collections.emptyList());
    schema.addProp(UR_SCHEMA_ATTR, name);
    return schema;
  }

  /**
   * Is this an unresolved schema.
   *
   * @param schema a schema
   * @return whether the schema is an unresolved schema
   */
  static boolean isUnresolvedSchema(final Schema schema) {
    return (schema.getType() == Schema.Type.RECORD && schema.getProp(UR_SCHEMA_ATTR) != null
        && UR_SCHEMA_NAME.equals(schema.getName()) && UR_SCHEMA_NS.equals(schema.getNamespace()));
  }

  /**
   * Get the unresolved schema name.
   *
   * @param schema an unresolved schema
   * @return the name of the unresolved schema
   */
  static String getUnresolvedSchemaName(final Schema schema) {
    if (!isUnresolvedSchema(schema)) {
      throw new IllegalArgumentException("Not a unresolved schema: " + schema);
    }
    return schema.getProp(UR_SCHEMA_ATTR);
  }

  /**
   * Is this an unresolved schema?
   */
  static boolean isFullyResolvedSchema(final Schema schema) {
    if (isUnresolvedSchema(schema)) {
      return false;
    } else {
      return Schemas.visit(schema, new IsResolvedSchemaVisitor());
    }
  }

  /**
   * Clone all provided schemas while resolving all unreferenced schemas.
   *
   * @param idlFile a parsed IDL file
   * @return a copy of idlFile with all schemas resolved
   */
  static IdlFile resolve(final IdlFile idlFile, String... schemaPropertiesToRemove) {
    return new IdlFile(resolve(idlFile.getProtocol(), schemaPropertiesToRemove), idlFile.getWarnings());
  }

  /**
   * Will clone the provided protocol while resolving all unreferenced schemas
   *
   * @param protocol a parsed protocol
   * @return a copy of the protocol with all schemas resolved
   */
  static Protocol resolve(final Protocol protocol, String... schemaPropertiesToRemove) {
    // Create an empty copy of the protocol
    Protocol result = new Protocol(protocol.getName(), protocol.getDoc(), protocol.getNamespace());
    protocol.getObjectProps().forEach(((JsonProperties) result)::addProp);

    ResolvingVisitor visitor = new ResolvingVisitor(null, protocol::getType, schemaPropertiesToRemove);
    Function<Schema, Schema> resolver = schema -> Schemas.visit(schema, visitor.withRoot(schema));

    // Resolve all schemata in the protocol.
    result.setTypes(protocol.getTypes().stream().map(resolver).collect(Collectors.toList()));
    Map<String, Protocol.Message> resultMessages = result.getMessages();
    protocol.getMessages().forEach((name, oldValue) -> {
      Protocol.Message newValue;
      if (oldValue.isOneWay()) {
        newValue = result.createMessage(oldValue.getName(), oldValue.getDoc(), oldValue,
            resolver.apply(oldValue.getRequest()));
      } else {
        Schema request = resolver.apply(oldValue.getRequest());
        Schema response = resolver.apply(oldValue.getResponse());
        Schema errors = resolver.apply(oldValue.getErrors());
        newValue = result.createMessage(oldValue.getName(), oldValue.getDoc(), oldValue, request, response, errors);
      }
      resultMessages.put(name, newValue);
    });
    return result;
  }
}
