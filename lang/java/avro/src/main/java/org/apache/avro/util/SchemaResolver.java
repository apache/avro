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
import org.apache.avro.JsonProperties;
import org.apache.avro.ParseContext;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.ENUM;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

/**
 * Utility class to resolve schemas that are unavailable at the point they are
 * referenced in the IDL.
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
   * Clone the provided schema while resolving all unreferenced schemas.
   *
   * @param parseContext the parse context with known names
   * @param schema       the schema to resolve
   * @return a copy of the schema with all schemas resolved
   */
  public static Schema resolve(final ParseContext parseContext, Schema schema) {
    if (schema == null) {
      return null;
    }
    ResolvingVisitor visitor = new ResolvingVisitor(schema, parseContext::resolve);
    return Schemas.visit(schema, visitor);
  }

  /**
   * Clone all provided schemas while resolving all unreferenced schemas.
   *
   * @param parseContext the parse context with known names
   * @param schemas      the schemas to resolve
   * @return a copy of all schemas with all schemas resolved
   */
  public static Collection<Schema> resolve(final ParseContext parseContext, Collection<Schema> schemas) {
    ResolvingVisitor visitor = new ResolvingVisitor(null, parseContext::resolve);
    return schemas.stream().map(schema -> Schemas.visit(schema, visitor.withRoot(schema))).collect(Collectors.toList());
  }

  /**
   * Will clone the provided protocol while resolving all unreferenced schemas
   *
   * @param parseContext the parse context with known names
   * @param protocol     the protocol to resolve
   * @return a copy of the protocol with all schemas resolved
   */
  public static Protocol resolve(ParseContext parseContext, final Protocol protocol) {
    // Create an empty copy of the protocol
    Protocol result = new Protocol(protocol.getName(), protocol.getDoc(), protocol.getNamespace());
    protocol.getObjectProps().forEach(((JsonProperties) result)::addProp);

    ResolvingVisitor visitor = new ResolvingVisitor(null, parseContext::resolve);
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
  public static final class ResolvingVisitor implements SchemaVisitor<Schema> {
    private static final Set<Schema.Type> CONTAINER_SCHEMA_TYPES = EnumSet.of(RECORD, ARRAY, MAP, UNION);
    private static final Set<Schema.Type> NAMED_SCHEMA_TYPES = EnumSet.of(RECORD, ENUM, FIXED);

    private final Function<String, Schema> symbolTable;
    private final Set<String> schemaPropertiesToRemove;
    private final IdentityHashMap<Schema, Schema> replace;

    private final Schema root;

    public ResolvingVisitor(final Schema root, final Function<String, Schema> symbolTable,
        String... schemaPropertiesToRemove) {
      this(root, symbolTable, new HashSet<>(Arrays.asList(schemaPropertiesToRemove)));
    }

    public ResolvingVisitor(final Schema root, final Function<String, Schema> symbolTable,
        Set<String> schemaPropertiesToRemove) {
      this.replace = new IdentityHashMap<>();
      this.symbolTable = symbolTable;
      this.schemaPropertiesToRemove = schemaPropertiesToRemove;

      this.root = root;
    }

    public ResolvingVisitor withRoot(Schema root) {
      return new ResolvingVisitor(root, symbolTable, schemaPropertiesToRemove);
    }

    @Override
    public SchemaVisitorAction visitTerminal(final Schema terminal) {
      Schema.Type type = terminal.getType();
      Schema newSchema;
      if (CONTAINER_SCHEMA_TYPES.contains(type)) {
        if (!replace.containsKey(terminal)) {
          throw new IllegalStateException("Schema " + terminal + " must be already processed");
        }
        return SchemaVisitorAction.CONTINUE;
      } else if (type == ENUM) {
        newSchema = Schema.createEnum(terminal.getName(), terminal.getDoc(), terminal.getNamespace(),
            terminal.getEnumSymbols(), terminal.getEnumDefault());
      } else if (type == FIXED) {
        newSchema = Schema.createFixed(terminal.getName(), terminal.getDoc(), terminal.getNamespace(),
            terminal.getFixedSize());
      } else {
        newSchema = Schema.create(type);
      }
      copyProperties(terminal, newSchema);
      replace.put(terminal, newSchema);
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
      first.getObjectProps().forEach((name, value) -> {
        if (!schemaPropertiesToRemove.contains(name)) {
          second.addProp(name, value);
        }
      });
    }

    @Override
    public SchemaVisitorAction visitNonTerminal(final Schema nt) {
      Schema.Type type = nt.getType();
      if (type == RECORD) {
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
            return replace.get(schema);
          });
          replace.put(nt, replacement);
        } else {
          // Create a clone without fields. Fields will be added in afterVisitNonTerminal.
          Schema newSchema = Schema.createRecord(nt.getName(), nt.getDoc(), nt.getNamespace(), nt.isError());
          copyProperties(nt, newSchema);
          replace.put(nt, newSchema);
        }
      }
      return SchemaVisitorAction.CONTINUE;
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
          }
        }
        return SchemaVisitorAction.CONTINUE;
      case UNION:
        List<Schema> types = nt.getTypes();
        List<Schema> newTypes = new ArrayList<>(types.size());
        for (Schema sch : types) {
          newTypes.add(replace.get(sch));
        }
        newSchema = Schema.createUnion(newTypes);
        break;
      case ARRAY:
        newSchema = Schema.createArray(replace.get(nt.getElementType()));
        break;
      case MAP:
        newSchema = Schema.createMap(replace.get(nt.getValueType()));
        break;
      default:
        throw new IllegalStateException("Illegal type " + type + ", schema " + nt);
      }
      copyProperties(nt, newSchema);
      replace.put(nt, newSchema);
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public Schema get() {
      return replace.get(root);
    }

    @Override
    public String toString() {
      return "ResolvingVisitor{symbolTable=" + symbolTable + ", schemaPropertiesToRemove=" + schemaPropertiesToRemove
          + ", replace=" + replace + '}';
    }
  }
}
