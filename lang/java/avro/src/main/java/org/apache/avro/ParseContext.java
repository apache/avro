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

import org.apache.avro.util.SchemaResolver;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class to define a name context, useful to reference schemata with. This
 * allows for the following:
 *
 * <ul>
 * <li>Provide a default namespace for nested contexts, as found for example in
 * JSON based schema definitions.</li>
 * <li>Find schemata by name, including primitives.</li>
 * <li>Collect new named schemata.</li>
 * </ul>
 *
 * <p>
 * Note: this class has no use for most Avro users, but is a key component when
 * implementing a schema parser.
 * </p>
 *
 * @see <a href="https://avro.apache.org/docs/current/specification/">JSON based
 *      schema definition</a>
 **/
public class ParseContext {
  private static final Map<String, Schema.Type> PRIMITIVES = new HashMap<>();

  static {
    PRIMITIVES.put("string", Schema.Type.STRING);
    PRIMITIVES.put("bytes", Schema.Type.BYTES);
    PRIMITIVES.put("int", Schema.Type.INT);
    PRIMITIVES.put("long", Schema.Type.LONG);
    PRIMITIVES.put("float", Schema.Type.FLOAT);
    PRIMITIVES.put("double", Schema.Type.DOUBLE);
    PRIMITIVES.put("boolean", Schema.Type.BOOLEAN);
    PRIMITIVES.put("null", Schema.Type.NULL);
  }

  private static final Set<Schema.Type> NAMED_SCHEMA_TYPES = EnumSet.of(Schema.Type.RECORD, Schema.Type.ENUM,
      Schema.Type.FIXED);
  private final Map<String, Schema> oldSchemas;
  private final Map<String, Schema> newSchemas;
  // Visible for use in JsonSchemaParser
  final NameValidator nameValidator;

  /**
   * Create a {@code ParseContext} for the default/{@code null} namespace, using
   * default name validation for new schemata.
   */
  public ParseContext() {
    this(NameValidator.UTF_VALIDATOR);
  }

  /**
   * Create a {@code ParseContext} using the specified name validation for new
   * schemata.
   */
  public ParseContext(NameValidator nameValidator) {
    this(nameValidator, new LinkedHashMap<>(), new LinkedHashMap<>());
  }

  private ParseContext(NameValidator nameValidator, Map<String, Schema> oldSchemas, Map<String, Schema> newSchemas) {
    this.nameValidator = nameValidator;
    this.oldSchemas = oldSchemas;
    this.newSchemas = newSchemas;
  }

  /**
   * Create a derived context using a different fallback namespace.
   *
   * @return a new context
   */
  public ParseContext namespace() {
    return new ParseContext(nameValidator, oldSchemas, newSchemas);
  }

  /**
   * Tell whether this context contains a schema with the given name.
   *
   * @param name a schema name
   * @return {@code true} if the context contains a schema with this name,
   *         {@code false} otherwise
   */
  public boolean contains(String name) {
    return PRIMITIVES.containsKey(name) || oldSchemas.containsKey(name) || newSchemas.containsKey(name);
  }

  /**
   * <p>
   * Resolve a schema by name.
   * </p>
   *
   * <p>
   * That is:
   * </p>
   *
   * <ul>
   * <li>If {@code fullName} is a primitive name, return a (new) schema for
   * it</li>
   * <li>Otherwise: resolve the schema in its own namespace and in the null
   * namespace (the former takes precedence)</li>
   * </ul>
   *
   * Resolving means that the schema is returned if known, and otherwise an
   * unresolved schema (a reference) is returned.
   *
   * @param fullName the full schema name to resolve
   * @return the schema
   * @throws SchemaParseException when the schema does not exist
   */
  public Schema resolve(String fullName) {
    Schema.Type type = PRIMITIVES.get(fullName);
    if (type != null) {
      return Schema.create(type);
    }

    Schema schema = getSchema(fullName);
    if (schema == null) {
      // Not found; attempt to resolve in the default namespace
      int lastDot = fullName.lastIndexOf('.');
      String name = fullName.substring(lastDot + 1);
      schema = getSchema(name);
    }

    return schema != null ? schema : SchemaResolver.unresolvedSchema(fullName);
  }

  private Schema getSchema(String fullName) {
    Schema schema = oldSchemas.get(fullName);
    if (schema == null) {
      schema = newSchemas.get(fullName);
    }
    return schema;
  }

  private boolean notEmpty(String str) {
    return str != null && !str.isEmpty();
  }

  /**
   * Put the schema into this context. This is an idempotent operation: it only
   * fails if this context already has a different schema with the same name.
   *
   * <p>
   * Note that although this method works for all types except for arrays, maps
   * and unions, all primitive types have already been defined upon construction.
   * This means you cannot redefine a 'long' with a logical timestamp type.
   * </p>
   *
   * @param schema the schema to put into the context
   */
  public void put(Schema schema) {
    if (!(NAMED_SCHEMA_TYPES.contains(schema.getType()))) {
      throw new AvroTypeException("You can only put a named schema into the context");
    }

    String fullName = requireValidFullName(schema.getFullName());

    Schema alreadyKnownSchema = oldSchemas.get(fullName);
    if (alreadyKnownSchema != null) {
      if (!schema.equals(alreadyKnownSchema)) {
        throw new SchemaParseException("Can't redefine: " + fullName);
      }
    } else {
      Schema previouslyAddedSchema = newSchemas.putIfAbsent(fullName, schema);
      if (previouslyAddedSchema != null && !previouslyAddedSchema.equals(schema)) {
        throw new SchemaParseException("Can't redefine: " + fullName);
      }
    }
  }

  private String requireValidFullName(String fullName) {
    String[] names = fullName.split("\\.");
    for (int i = 0; i < names.length - 1; i++) {
      validateName(names[i], "Namespace part");
    }
    validateName(names[names.length - 1], "Name");
    return fullName;
  }

  private void validateName(String name, String what) {
    NameValidator.Result result = nameValidator.validate(name);
    if (!result.isOK()) {
      throw new SchemaParseException(what + " \"" + name + "\" is invalid: " + result.getErrors());
    }
  }

  public boolean hasNewSchemas() {
    return !newSchemas.isEmpty();
  }

  public void commit() {
    oldSchemas.putAll(newSchemas);
    newSchemas.clear();
  }

  public void rollback() {
    newSchemas.clear();
  }

  /**
   * Return all known types by their fullname.
   *
   * @return a map of all types by their name
   */
  public Map<String, Schema> typesByName() {
    LinkedHashMap<String, Schema> result = new LinkedHashMap<>();
    result.putAll(oldSchemas);
    result.putAll(newSchemas);
    return result;
  }
}
