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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Class to define a name context, useful to reference schemata with. This
 * allows for the following:
 *
 * <ul>
 * <li>Collect new named schemata.</li>
 * <li>Find schemata by name, including primitives.</li>
 * <li>Find schemas that do not exist yet.</li>
 * <li>Resolve references to schemas that didn't exist yet when first used.</li>
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
  /**
   * Collection of old schemata. Can contain unresolved references if !isResolved.
   */
  private final Map<String, Schema> oldSchemas;
  /**
   * Collection of new schemata. Can contain unresolved references.
   */
  private final Map<String, Schema> newSchemas;
  /**
   * The name validator to use.
   */
  // Visible for use in JsonSchemaParser
  final NameValidator nameValidator;
  /**
   * Visitor that was used to resolve schemata with. If not available, some
   * schemata in {@code oldSchemas} may not be fully resolved. If available, all
   * schemata in {@code oldSchemas} are resolved, and {@code newSchemas} is empty.
   * After visiting a schema, it can return the corresponding resolved schema for
   * a schema that possibly contains unresolved references.
   */
  private SchemaResolver.ResolvingVisitor resolvingVisitor;

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
    resolvingVisitor = null;
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
   * <p>
   * Note: as an unresolved reference might be returned, the schema is not
   * directly usable. Please {@link #put(Schema)} the schema using it in the
   * context. The {@link SchemaParser} and protocol parsers will ensure you'll
   * only get a resolved schema that is usable.
   * </p>
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
   * until {@link #resolveAllSchemas()} has been called.
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
      resolvingVisitor = null;
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

  public SchemaParser.ParseResult commit(Schema mainSchema) {
    List<Schema> parsedNamedSchemas = new ArrayList<>(newSchemas.values());
    SchemaParser.ParseResult parseResult = new SchemaParser.ParseResult() {
      @Override
      public Schema mainSchema() {
        return mainSchema == null ? null : resolve(mainSchema);
      }

      @Override
      public List<Schema> parsedNamedSchemas() {
        return parsedNamedSchemas.stream().map(ParseContext.this::resolve).collect(Collectors.toList());
      }
    };
    commit();
    return parseResult;
  }

  public void rollback() {
    newSchemas.clear();
  }

  /**
   * Resolve all (named) schemas that were parsed. This resolves all forward
   * references, even if parsed from different files. Note: the context must be
   * committed for this method to work.
   *
   * @return all parsed schemas, in the order they were parsed
   * @throws AvroTypeException if a schema reference cannot be resolved
   */
  public List<Schema> resolveAllSchemas() {
    ensureSchemasAreResolved();

    return new ArrayList<>(oldSchemas.values());
  }

  private void ensureSchemasAreResolved() {
    if (hasNewSchemas()) {
      throw new IllegalStateException("Schemas cannot be resolved unless the ParseContext is committed.");
    }
    if (resolvingVisitor == null) {
      NameValidator saved = Schema.getNameValidator();
      try {
        // Ensure we use the same validation when copying schemas as when they were
        // defined.
        Schema.setNameValidator(nameValidator);
        SchemaResolver.ResolvingVisitor visitor = new SchemaResolver.ResolvingVisitor(oldSchemas::get);
        oldSchemas.values().forEach(schema -> Schemas.visit(schema, visitor));
        // Before this point is where we can get exceptions due to resolving failures.
        for (Map.Entry<String, Schema> entry : oldSchemas.entrySet()) {
          entry.setValue(visitor.getResolved(entry.getValue()));
        }
        resolvingVisitor = visitor;
      } finally {
        Schema.setNameValidator(saved);
      }
    }
  }

  /**
   * Resolve unresolved references in a schema <em>that was parsed for this
   * context</em> using the types known to this context. Note: this method will
   * ensure all known schemas are resolved, or throw, and thus requires the
   * context to be committed.
   *
   * @param schema the schema resolve
   * @return the fully resolved schema
   * @throws AvroTypeException if a schema reference cannot be resolved
   */
  public Schema resolve(Schema schema) {
    ensureSchemasAreResolved();

    // As all (named) schemas are resolved now, we know:
    // — All named types are either in oldSchemas or unknown.
    // — All unnamed types can be visited&resolved without validation.

    if (NAMED_SCHEMA_TYPES.contains(schema.getType()) && schema.getFullName() != null) {
      return requireNonNull(oldSchemas.get(schema.getFullName()), () -> "Unknown schema: " + schema.getFullName());
    } else {
      // Unnamed or anonymous schema
      // (protocol message request parameters are anonymous records)
      Schemas.visit(schema, resolvingVisitor); // This field is set, as ensureSchemasAreResolved(); was called.
      return resolvingVisitor.getResolved(schema);
    }
  }

  /**
   * Return all known types by their fullname. Warning: this returns all types,
   * even uncommitted ones, including unresolved references!
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
