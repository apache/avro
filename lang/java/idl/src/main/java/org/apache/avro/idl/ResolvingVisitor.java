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

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.ENUM;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

/**
 * This visitor creates clone of the visited Schemata, minus the specified
 * schema properties, and resolves all unresolved schemas.
 */
public final class ResolvingVisitor implements SchemaVisitor<Schema> {
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
      if (SchemaResolver.isUnresolvedSchema(nt)) {
        // unresolved schema will get a replacement that we already encountered,
        // or we will attempt to resolve.
        final String unresolvedSchemaName = SchemaResolver.getUnresolvedSchemaName(nt);
        Schema resSchema = symbolTable.apply(unresolvedSchemaName);
        if (resSchema == null) {
          throw new AvroTypeException("Unable to resolve " + unresolvedSchemaName);
        }
        Schema replacement = replace.computeIfAbsent(resSchema, schema -> {
          Schemas.visit(schema, this);
          return replace.get(schema);
        });
        replace.put(nt, replacement);
      } else {
        // create a fieldless clone. Fields will be added in afterVisitNonTerminal.
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
      if (!SchemaResolver.isUnresolvedSchema(nt)) {
        newSchema = replace.get(nt);
        // Check if we've already handled the replacement schema with a
        // reentrant call to visit(...) from within the visitor.
        if (!newSchema.hasFields()) {
          List<Schema.Field> fields = nt.getFields();
          List<Schema.Field> newFields = new ArrayList<>(fields.size());
          for (Schema.Field field : fields) {
            newFields.add(new Field(field, replace.get(field.schema())));
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
