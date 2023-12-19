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
import org.apache.avro.util.Schemas;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Class to define a name context, useful to reference schemata with. This
 * allows for the following:
 *
 * <ul>
 * <li>Find schemata by name, including primitives.</li>
 * <li>Find schemas that do not exist yet. Use with {@link #resolveAllTypes()}
 * to ensure resulting schemas are usable.</li>
 * <li>Collect new named schemata.</li>
 * </ul>
 *
 * <p>
 * This class is NOT thread-safe.
 * </p>
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
  boolean isResolved;

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
    this(requireNonNull(nameValidator), new LinkedHashMap<>(), new LinkedHashMap<>());
  }

  private ParseContext(NameValidator nameValidator, Map<String, Schema> oldSchemas, Map<String, Schema> newSchemas) {
    this.nameValidator = nameValidator;
    this.oldSchemas = oldSchemas;
    this.newSchemas = newSchemas;
    isResolved = false;
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
   * Find a schema by name and namespace.
   * </p>
   *
   * <p>
   * That is:
   * </p>
   *
   * <ol>
   * <li>If {@code name} is a primitive name, return a (new) schema for it</li>
   * <li>Otherwise, determine the full schema name (using the given
   * {@code namespace} if necessary), and find it</li>
   * <li>If no schema was found and {@code name} is a simple name, find the schema
   * in the default (null) namespace</li>
   * <li>If still no schema was found, return an unresolved reference for the full
   * schema name (see step 2)</li>
   * </ol>
   *
   * @param name      the schema name to find
   * @param namespace the namespace to find the schema against
   * @return the schema, or an unresolved reference
   */
  public Schema find(String name, String namespace) {
    Schema.Type type = PRIMITIVES.get(name);
    if (type != null) {
      return Schema.create(type);
    }

    String fullName = fullName(name, namespace);
    Schema schema = getNamedSchema(fullName);
    if (schema == null) {
      schema = getNamedSchema(name);
    }

    return schema != null ? schema : SchemaResolver.unresolvedSchema(fullName);
  }

  private String fullName(String name, String namespace) {
    if (namespace != null && name.lastIndexOf('.') < 0) {
      return namespace + "." + name;
    }
    return name;
  }

  /**
   * Get a schema by name. Note that the schema might not (yet) be resolved/usable
   * until {@link #resolveAllTypes()} has been called.
   *
   * @param fullName a full schema name
   * @return the schema, if known
   */
  public Schema getNamedSchema(String fullName) {
    Schema schema = oldSchemas.get(fullName);
    if (schema == null) {
      schema = newSchemas.get(fullName);
    }
    return schema;
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
      isResolved = false;
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

  private void validateName(String name, String typeOfName) {
    NameValidator.Result result = nameValidator.validate(name);
    if (!result.isOK()) {
      throw new SchemaParseException(typeOfName + " \"" + name + "\" is invalid: " + result.getErrors());
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
   * Resolve all (named) schemas that were parsed. This resolves all forward
   * references, even if parsed from different files.
   *
   * @return all parsed schemas, in the order they were parsed
   * @throws AvroTypeException if a reference cannot be resolved
   */
  public List<Schema> resolveAllTypes() {
    if (hasNewSchemas()) {
      throw new IllegalStateException("Types cannot be resolved unless the ParseContext is committed.");
    }

    if (!isResolved) {
      NameValidator saved = Schema.getNameValidator();
      try {
        Schema.setNameValidator(nameValidator); // Ensure we use the same validation.
        HashMap<String, Schema> result = new LinkedHashMap<>(oldSchemas);
        SchemaResolver.ResolvingVisitor visitor = new SchemaResolver.ResolvingVisitor(null, result::get, false);
        Function<Schema, Schema> resolver = schema -> Schemas.visit(schema, visitor.withRoot(schema));
        for (Map.Entry<String, Schema> entry : result.entrySet()) {
          entry.setValue(resolver.apply(entry.getValue()));
        }
        oldSchemas.putAll(result);
        isResolved = true;
      } finally {
        Schema.setNameValidator(saved);
      }
    }

    return new ArrayList<>(oldSchemas.values());
  }

  /**
   * Try to resolve unresolved references in a schema using the types known to
   * this context. It is advisable to call {@link #resolveAllTypes()} first if you
   * want the returned types to be stable.
   *
   * @param schema the schema resolve
   * @return the fully resolved schema if possible, {@code null} otherwise
   */
  public Schema tryResolve(Schema schema) {
    if (schema == null) {
      return null;
    }
    return resolve(schema, true);
  }

  /**
   * Resolve unresolved references in a schema using the types known to this
   * context. It is advisable to call {@link #resolveAllTypes()} first if you want
   * the returned types to be stable.
   *
   * @param schema the schema resolve
   * @return the fully resolved schema
   * @throws AvroTypeException if the schema cannot be resolved
   */
  public Schema resolve(Schema schema) {
    return resolve(schema, false);
  }

  public Schema resolve(Schema schema, boolean returnNullUponFailure) {
    NameValidator saved = Schema.getNameValidator();
    try {
      Schema.setNameValidator(nameValidator); // Ensure we use the same validation.
      return Schemas.visit(schema,
          new SchemaResolver.ResolvingVisitor(schema, this::getNamedSchema, returnNullUponFailure));
    } finally {
      Schema.setNameValidator(saved);
    }
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
