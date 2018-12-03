/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.schema;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * this visitor will create a clone of the original Schema with docs and other nonessential fields stripped
 * by default. what attributes are copied is customizable.
 */
public final class CloningVisitor implements SchemaVisitor<Schema> {

  private final IdentityHashMap<Schema, Schema> replace = new IdentityHashMap<>();

  private final Schema root;

  private final PropertyCopier copyProperties;

  private final boolean copyDocs;

  public interface PropertyCopier {
    void copy(Schema first, Schema second);

    void copy(Schema.Field first, Schema.Field second);
  }

  /**
   * copy only serialization necessary fields.
   *
   * @param root
   */
  public CloningVisitor(final Schema root) {
    this(new PropertyCopier() {
      @Override
      public void copy(final Schema first, final Schema second) {
        Schemas.copyLogicalTypes(first, second);
        Schemas.copyAliases(first, second);
      }

      @Override
      public void copy(final Schema.Field first, final Schema.Field second) {
        Schemas.copyAliases(first, second);
      }
    }, false, root);
  }

  public CloningVisitor(final PropertyCopier copyProperties, final boolean copyDocs, final Schema root) {
    this.copyProperties = copyProperties;
    this.copyDocs = copyDocs;
    this.root = root;
  }

  @Override
  public SchemaVisitorAction visitTerminal(final Schema terminal) {
    Schema.Type type = terminal.getType();
    Schema newSchema;
    switch (type) {
      case RECORD: // recursion.
      case ARRAY:
      case MAP:
      case UNION:
        if (!replace.containsKey(terminal)) {
          throw new IllegalStateException("Schema " + terminal + " must be already processed");
        }
        return SchemaVisitorAction.CONTINUE;
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case NULL:
      case STRING:
        newSchema = Schema.create(type);
        break;
      case ENUM:
        newSchema = Schema.createEnum(terminal.getName(), copyDocs ? terminal.getDoc() : null,
            terminal.getNamespace(), terminal.getEnumSymbols());
        break;
      case FIXED:
        newSchema = Schema.createFixed(terminal.getName(), copyDocs ? terminal.getDoc() : null,
            terminal.getNamespace(), terminal.getFixedSize());
        break;
      default:
        throw new IllegalStateException("Unsupported schema " + terminal);
    }
    copyProperties.copy(terminal, newSchema);
    replace.put(terminal, newSchema);
    return SchemaVisitorAction.CONTINUE;
  }

  @Override
  public SchemaVisitorAction visitNonTerminal(final Schema nt) {
    Schema.Type type = nt.getType();
    if (type == RECORD) {
      Schema newSchema = Schema.createRecord(nt.getName(), copyDocs ? nt.getDoc() : null,
          nt.getNamespace(), nt.isError());
      copyProperties.copy(nt, newSchema);
      replace.put(nt, newSchema);
    }
    return SchemaVisitorAction.CONTINUE;
  }

  @Override
  public SchemaVisitorAction afterVisitNonTerminal(final Schema nt) {
    Schema.Type type = nt.getType();
    Schema newSchema;
    switch (type) {
      case RECORD:
        newSchema = replace.get(nt);
        List<Schema.Field> fields = nt.getFields();
        List<Schema.Field> newFields = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
          Schema.Field newField = new Schema.Field(field.name(), replace.get(field.schema()),
              copyDocs ? field.doc() : null, field.defaultVal(), field.order());
          copyProperties.copy(field, newField);
          newFields.add(newField);
        }
        newSchema.setFields(newFields);
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
    copyProperties.copy(nt, newSchema);
    replace.put(nt, newSchema);
    return SchemaVisitorAction.CONTINUE;
  }

  @Override
  public Schema get() {
    return replace.get(root);
  }

  @Override
  public String toString() {
    return "CloningVisitor{" + "replace=" + replace + ", root=" + root + '}';
  }

}
