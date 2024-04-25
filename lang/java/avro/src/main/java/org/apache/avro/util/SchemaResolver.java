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
package org.apache.avro.util;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.ENUM;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

/**
 * Utility class to resolve schemas that are unavailable at the point they are
 * referenced in a schema file. This class is meant for internal use: use at
 * your own risk!
 */
public final class SchemaResolver {

  private SchemaResolver() {
  }

  private static final String UR_SCHEMA_ATTR = "org.apache.avro.idl.unresolved.name";

  private static final String UR_SCHEMA_NAME = "UnresolvedSchema";

  private static final String UR_SCHEMA_NS = "org.apache.avro.compiler";

  private static final AtomicInteger COUNTER = new AtomicInteger();

  /**
   * Create a schema to represent an "unresolved" schema. (used to represent a
   * schema whose definition does not exist, yet).
   *
   * @param name a schema name
   * @return an unresolved schema for the given name
   */
  public static Schema unresolvedSchema(final String name) {
    Schema schema = Schema.createRecord(UR_SCHEMA_NAME + '_' + COUNTER.getAndIncrement(), "unresolved schema",
        UR_SCHEMA_NS, false, Collections.emptyList());
    schema.addProp(UR_SCHEMA_ATTR, name);
    return schema;
  }

  /**
   * Is this an unresolved schema.
   *
   * @param schema a schema
   * @return whether the schema is an unresolved schema
   */
  public static boolean isUnresolvedSchema(final Schema schema) {
    return (schema.getType() == Schema.Type.RECORD && schema.getProp(UR_SCHEMA_ATTR) != null && schema.getName() != null
        && schema.getName().startsWith(UR_SCHEMA_NAME) && UR_SCHEMA_NS.equals(schema.getNamespace()));
  }

  /**
   * Get the unresolved schema name.
   *
   * @param schema an unresolved schema
   * @return the name of the unresolved schema
   */
  public static String getUnresolvedSchemaName(final Schema schema) {
    if (!isUnresolvedSchema(schema)) {
      throw new IllegalArgumentException("Not a unresolved schema: " + schema);
    }
    return schema.getProp(UR_SCHEMA_ATTR);
  }

  /**
   * Is this an unresolved schema?
   */
  public static boolean isFullyResolvedSchema(final Schema schema) {
    if (isUnresolvedSchema(schema)) {
      return false;
    } else {
      return Schemas.visit(schema, new IsResolvedSchemaVisitor());
    }
  }

  /**
   * This visitor checks if the current schema is fully resolved.
   */
  public static final class IsResolvedSchemaVisitor implements SchemaVisitor<Boolean> {
    boolean hasUnresolvedParts;

    IsResolvedSchemaVisitor() {
      hasUnresolvedParts = false;
    }

    @Override
    public SchemaVisitorAction visitTerminal(Schema terminal) {
      hasUnresolvedParts = isUnresolvedSchema(terminal);
      return hasUnresolvedParts ? SchemaVisitorAction.TERMINATE : SchemaVisitorAction.CONTINUE;
    }

    @Override
    public SchemaVisitorAction visitNonTerminal(Schema nonTerminal) {
      hasUnresolvedParts = isUnresolvedSchema(nonTerminal);
      if (hasUnresolvedParts) {
        return SchemaVisitorAction.TERMINATE;
      }
      if (nonTerminal.getType() == Schema.Type.RECORD && !nonTerminal.hasFields()) {
        // We're still initializing the type...
        return SchemaVisitorAction.SKIP_SUBTREE;
      }
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public SchemaVisitorAction afterVisitNonTerminal(Schema nonTerminal) {
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public Boolean get() {
      return !hasUnresolvedParts;
    }
  }

  /**
   * This visitor creates clone of the visited Schemata, minus the specified
   * schema properties, and resolves all unresolved schemas.
   */
  public static final class ResolvingVisitor implements SchemaVisitor<Void> {
    private static final Set<Schema.Type> CONTAINER_SCHEMA_TYPES = EnumSet.of(RECORD, ARRAY, MAP, UNION);
    private static final Set<Schema.Type> NAMED_SCHEMA_TYPES = EnumSet.of(RECORD, ENUM, FIXED);

    private final Function<String, Schema> symbolTable;
    private final IdentityHashMap<Schema, Schema> replace;

    public ResolvingVisitor(final Function<String, Schema> symbolTable) {
      this.replace = new IdentityHashMap<>();
      this.symbolTable = symbolTable;
    }

    @Override
    public SchemaVisitorAction visitTerminal(final Schema terminal) {
      Schema.Type type = terminal.getType();
      if (CONTAINER_SCHEMA_TYPES.contains(type)) {
        if (!replace.containsKey(terminal)) {
          throw new IllegalStateException("Schema " + terminal + " must be already processed");
        }
      } else {
        replace.put(terminal, terminal);
      }
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public SchemaVisitorAction visitNonTerminal(final Schema nt) {
      Schema.Type type = nt.getType();
      if (type == RECORD && !replace.containsKey(nt)) {
        if (isUnresolvedSchema(nt)) {
          // unresolved schema will get a replacement that we already encountered,
          // or we will attempt to resolve.
          final String unresolvedSchemaName = getUnresolvedSchemaName(nt);
          Schema resSchema = symbolTable.apply(unresolvedSchemaName);
          if (resSchema == null) {
            throw new AvroTypeException("Undefined schema: " + unresolvedSchemaName);
          }
          Schema replacement = replace.computeIfAbsent(resSchema, schema -> {
            Schemas.visit(schema, this);
            return replace.get(schema); // This is not what the visitor returns!
          });
          replace.put(nt, replacement);
        } else {
          // Create a clone without fields or properties. They will be added in
          // afterVisitNonTerminal, as they can both create circular references.
          // (see org.apache.avro.TestCircularReferences as an example)
          replace.put(nt, Schema.createRecord(nt.getName(), nt.getDoc(), nt.getNamespace(), nt.isError()));
        }
      }
      return SchemaVisitorAction.CONTINUE;
    }

    public void copyProperties(final Schema first, final Schema second) {
      // Logical type
      Optional.ofNullable(first.getLogicalType()).ifPresent(logicalType -> logicalType.addToSchema(second));

      // Aliases (if applicable)
      if (NAMED_SCHEMA_TYPES.contains(first.getType())) {
        first.getAliases().forEach(second::addAlias);
      }

      // Other properties
      first.getObjectProps().forEach(second::addProp);
    }

    @Override
    public SchemaVisitorAction afterVisitNonTerminal(final Schema nt) {
      Schema.Type type = nt.getType();
      Schema newSchema;
      switch (type) {
      case RECORD:
        if (!isUnresolvedSchema(nt)) {
          newSchema = replace.get(nt);
          // Check if we've already handled the replacement schema with a
          // reentrant call to visit(...) from within the visitor.
          if (!newSchema.hasFields()) {
            List<Schema.Field> fields = nt.getFields();
            List<Schema.Field> newFields = new ArrayList<>(fields.size());
            for (Schema.Field field : fields) {
              newFields.add(new Schema.Field(field, replace.get(field.schema())));
            }
            newSchema.setFields(newFields);
            copyProperties(nt, newSchema);
          }
        }
        return SchemaVisitorAction.CONTINUE;
      case UNION:
        List<Schema> types = nt.getTypes();
        List<Schema> newTypes = new ArrayList<>(types.size());
        for (Schema sch : types) {
          newTypes.add(requireNonNull(replace.get(sch)));
        }
        newSchema = Schema.createUnion(newTypes);
        break;
      case ARRAY:
        newSchema = Schema.createArray(requireNonNull(replace.get(nt.getElementType())));
        break;
      case MAP:
        newSchema = Schema.createMap(requireNonNull(replace.get(nt.getValueType())));
        break;
      default:
        throw new IllegalStateException("Illegal type " + type + ", schema " + nt);
      }
      copyProperties(nt, newSchema);
      replace.put(nt, newSchema);
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public Void get() {
      return null;
    }

    public Schema getResolved(Schema schema) {
      return requireNonNull(replace.get(schema),
          () -> "Unknown schema: " + schema.getFullName() + ". Was it resolved before?");
    }

    @Override
    public String toString() {
      return "ResolvingVisitor{symbolTable=" + symbolTable + ", replace=" + replace + '}';
    }
  }
}
