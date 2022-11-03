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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Class to define a name context, useful to reference types with. This allows
 * for the following:
 * <li>Cache and re-use primitive schemas when they do not set properties.</li>
 * <li>Provide a default namespace for nested contexts (as the JSON Schema spec
 * does).</li>
 * <li>Allow previously defined named types or primitive types to be referenced
 * by name.</li>
 *
 * <p>
 * Note that although this class has no direct use, it is useful when (for
 * example) writing a schema parser.
 * </p>
 **/
public class NameContext {
  private static final Set<String> PRIMITIVES = new HashSet<>();
  private static final Set<Schema.Type> NAMED_SCHEMA_TYPES = EnumSet.of(Schema.Type.RECORD, Schema.Type.ENUM,
      Schema.Type.FIXED);
  private static final Function<String, Schema> DEFAULT_FALLBACK = fullName -> {
    throw new SchemaParseException("Undefined name: " + fullName);
  };

  static {
    PRIMITIVES.add("null");
    PRIMITIVES.add("boolean");
    PRIMITIVES.add("int");
    PRIMITIVES.add("long");
    PRIMITIVES.add("float");
    PRIMITIVES.add("double");
    PRIMITIVES.add("bytes");
    PRIMITIVES.add("string");
  }

  private final Map<String, Schema> schemas;
  private final String namespace;

  /**
   * Create a {@code NameContext} for the default/{@code null} namespace.
   */
  public NameContext() {
    this(null);
  }

  /**
   * Create a {@code NameContext} for the specified namespace.
   */
  public NameContext(String namespace) {
    this.schemas = new LinkedHashMap<>();
    this.namespace = namespace;
    schemas.put("null", Schema.create(Schema.Type.NULL));
    schemas.put("boolean", Schema.create(Schema.Type.BOOLEAN));
    schemas.put("int", Schema.create(Schema.Type.INT));
    schemas.put("long", Schema.create(Schema.Type.LONG));
    schemas.put("float", Schema.create(Schema.Type.FLOAT));
    schemas.put("double", Schema.create(Schema.Type.DOUBLE));
    schemas.put("bytes", Schema.create(Schema.Type.BYTES));
    schemas.put("string", Schema.create(Schema.Type.STRING));
  }

  /**
   * Create a derived {@code NameContext} (which uses the same schema collection),
   * using the specified namespace.
   *
   * @param schemas   the schema collection to use
   * @param namespace the fallback namespace to resolve names with
   */
  private NameContext(Map<String, Schema> schemas, String namespace) {
    this.schemas = schemas;
    this.namespace = notEmpty(namespace) ? namespace : null;
  }

  /**
   * Create a derived context using a different fallback namespace.
   *
   * @param namespace the fallback namespace to resolve names with
   * @return a new context
   */
  public NameContext namespace(String namespace) {
    return new NameContext(schemas, namespace);
  }

  /**
   * Return the fallback namespace.
   *
   * @return the namespace
   */
  public String namespace() {
    return namespace;
  }

  public String getQualifiedName(String name) {
    int lastDot = name.lastIndexOf('.');
    String simpleName = name.substring(lastDot + 1);
    String space = lastDot < 0 ? null : name.substring(0, lastDot);

    // Determine if full name must be written. There are 2 cases for true :
    // defaultSpace != from this.space or name is already a Schema.Type (int,
    // array, ...)

    if (namespace != null && namespace.equals(space)) {
      for (Schema.Type schemaType : Schema.Type.values()) {
        if (schemaType.getName().equals(name)) {
          // name is a 'Type', so space must be written
          return name;
        }
      }
      // this.namespace == space
      return simpleName;
    }
    // this.namespace != space, so space must be written.
    return name;
  }

  /**
   * Tell whether this context contains the schema.
   *
   * @param schema a schema
   * @return {@code true} if the context contains the schema, {@code false}
   *         otherwise
   */
  public boolean contains(Schema schema) {
    return schema.equals(schemas.get(schema.getFullName()));
  }

  /**
   * Tell whether this context contains a schema with the given fullname.
   *
   * @param fullName a schema name
   * @return {@code true} if the context contains a schema with thisname,
   *         {@code false} otherwise
   */
  public boolean contains(String fullName) {
    return schemas.containsKey(fullName);
  }

  /**
   * Resolve a schema by name and namespace. Resolves the fullname first:
   *
   * <li>If {@code name} is a primitive name and {@code namespace} is empty, use
   * it as fullname (never use the fallback namespace for primitive names))</li>
   * <li>If {@code name} contains a dot, use it as fullname</li>
   * <li>Otherwise: if {@code namespace} is not empty, use
   * {@code name + "." + namespace} as fullname</li>
   * <li>Otherwise: if the fallback namespace is not empty, use
   * {@code name + "." + fallback} as fullname</li>
   * <li>Otherwise use {@code name} as fullname (in the default namespace)</li>
   *
   * @param name      the schema name to resolve
   * @param namespace the schema namespace (if any) to resolve
   * @return the schema
   * @throws SchemaParseException when the schema does not exist
   */
  public Schema resolve(String name, String namespace) {
    return resolveWithFallback(name, namespace, DEFAULT_FALLBACK);
  }

  /**
   * Resolve a schema by name and namespace. Resolves the fullname first:
   *
   * <li>If {@code name} is a primitive name and {@code namespace} is empty, use
   * it as fullname (never use the fallback namespace for primitive names))</li>
   * <li>If {@code name} contains a dot, use it as fullname</li>
   * <li>Otherwise: if {@code namespace} is not empty, use
   * {@code name + "." + namespace} as fullname</li>
   * <li>Otherwise: if the fallback namespace is not empty, use
   * {@code name + "." + fallback} as fullname</li>
   * <li>Otherwise use {@code name} as fullname (in the default namespace)</li>
   *
   * @param name      the schema name to resolve
   * @param namespace the schema namespace (if any) to resolve
   * @param fallback  a fallback method to be called with the full schema name if
   *                  the schema is unknown
   * @return the schema
   * @throws SchemaParseException when the schema does not exist
   */
  public Schema resolveWithFallback(String name, String namespace, Function<String, Schema> fallback) {
    return getByFullname(resolveName(name, namespace), fallback);
  }

  /**
   * Resolve a schema by name. Resolves the fullname first:
   *
   * <li>If {@code name} is a primitive name, use it as fullname (never use the
   * fallback namespace for primitive names))</li>
   * <li>If {@code name} contains a dot, use it as fullname</li>
   * <li>Otherwise: if the fallback namespace is not empty, use
   * {@code name + "." + fallback} as fullname</li>
   * <li>Otherwise use {@code name} as fullname (in the default namespace)</li>
   *
   * @param name the schema name to resolve
   * @return the schema
   * @throws SchemaParseException when the schema does not exist
   */
  public Schema resolve(String name) {
    return resolveWithFallback(name, null, DEFAULT_FALLBACK);
  }

  /**
   * Resolve a schema by name. Resolves the fullname first:
   *
   * <li>If {@code name} is a primitive name, use it as fullname (never use the
   * fallback namespace for primitive names))</li>
   * <li>If {@code name} contains a dot, use it as fullname</li>
   * <li>Otherwise: if the fallback namespace is not empty, use
   * {@code name + "." + fallback} as fullname</li>
   * <li>Otherwise use {@code name} as fullname (in the default namespace)</li>
   *
   * @param name     the schema name to resolve
   * @param fallback a fallback method to be called with the full schema name if
   *                 the schema is unknown
   * @return the schema
   */
  public Schema resolveWithFallback(String name, Function<String, Schema> fallback) {
    return resolveWithFallback(name, null, fallback);
  }

  private Schema getByFullname(String fullName, Function<String, Schema> fallback) {
    Schema schema = schemas.get(fullName);
    if (schema == null) {
      int lastDot = fullName.lastIndexOf('.');
      String simpleName = fullName.substring(lastDot + 1);
      schema = schemas.get(simpleName);
    }
    if (schema == null) {
      return fallback.apply(fullName);
    }
    return schema;
  }

  // Visible for testing
  String resolveName(String name, String space) {
    if (PRIMITIVES.contains(name) && !notEmpty(space)) {
      return name;
    }
    int lastDot = name.lastIndexOf('.');
    if (lastDot < 0) { // short name
      if (!notEmpty(space)) {
        space = namespace;
      }
      if (notEmpty(space)) {
        return space + "." + name;
      }
    }
    return name;
  }

  private boolean notEmpty(String str) {
    return str != null && !"".equals(str);
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
    String fullName = schema.getFullName();
    Schema previouslyAddedSchema = schemas.putIfAbsent(fullName, schema);
    if (previouslyAddedSchema != null && !previouslyAddedSchema.equals(schema)) {
      throw new SchemaParseException("Can't redefine: " + fullName);
    }
  }

  /**
   * Return all known types by their fullname.
   *
   * @return a map of all types by their name
   */
  public Map<String, Schema> typesByName() {
    Map<String, Schema> namedSchemas = new LinkedHashMap<>();
    for (Schema schema : schemas.values()) {
      if (NAMED_SCHEMA_TYPES.contains(schema.getType())) {
        namedSchemas.put(schema.getFullName(), schema);
      }
    }
    return namedSchemas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NameContext that = (NameContext) o;
    return schemas.equals(that.schemas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemas);
  }
}
