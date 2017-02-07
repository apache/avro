/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.idl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.schema.Schemas;

/**
 * Utility class to resolve schemas that are unavailable at the time they are referenced in the IDL.
 */

final class SchemaResolver {

  private SchemaResolver() {
  }

  private static final String UR_SCHEMA_ATTR = "org.apache.avro.compiler.idl.unresolved.name";

  private static final String UR_SCHEMA_NAME = "UnresolvedSchema";

  private static final String UR_SCHEMA_NS = "org.apache.avro.compiler";

  static Schema unresolvedSchema(final String name) {


    Schema schema = Schema.createRecord(UR_SCHEMA_NAME, "unresolved schema",
            UR_SCHEMA_NS, false, Collections.EMPTY_LIST);
    schema.addProp(UR_SCHEMA_ATTR, name);
    return schema;
  }

  static boolean isUnresolvedSchema(final Schema schema) {
    return (schema.getType() == Schema.Type.RECORD && schema.getProp(UR_SCHEMA_ATTR) != null
            && UR_SCHEMA_NAME.equals(schema.getName())
            && UR_SCHEMA_NS.equals(schema.getNamespace()));
  }

  static String getUnresolvedSchemaName(final Schema schema) {
    if (!isUnresolvedSchema(schema)) {
      throw new IllegalArgumentException("Not a unresolved schema: " + schema);
    }
    String name = schema.getProp(UR_SCHEMA_ATTR);
    if (name == null) {
      throw new IllegalArgumentException("Schema " + schema + " must have attribute: " + UR_SCHEMA_ATTR);
    } else {
      return name;
    }
  }

  /**
   * Resolve all unresolved schema references from a protocol.
   * @param protocol - the protocol with unresolved schema references.
   * @return - a new protocol instance based on the provided protocol with all unresolved schema references resolved.
   */
  static Protocol resolve(final Protocol protocol) {
    Protocol result = new Protocol(protocol.getName(), protocol.getDoc(), protocol.getNamespace());
    final Collection<Schema> types = protocol.getTypes();
    List<Schema> newSchemas = new ArrayList(types.size());
    Map<String, Schema> resolved = new HashMap<String, Schema>();
    for (Schema schema : types) {
      newSchemas.add(resolve(schema, protocol, resolved));
    }
    result.setTypes(newSchemas); // replace types with resolved ones

    for (Map.Entry<String, Protocol.Message> entry : protocol.getMessages().entrySet()) {
      Protocol.Message value = entry.getValue();
      Protocol.Message nvalue;
      if (value.isOneWay()) {
        Schema request = value.getRequest();
        nvalue = result.createMessage(value.getName(), value.getDoc(),
                value.getObjectProps(), getResolvedSchema(request, resolved));
      } else {
        Schema request = value.getRequest();
        Schema response = value.getResponse();
        Schema errors = value.getErrors();
        nvalue = result.createMessage(value.getName(), value.getDoc(),
                value.getObjectProps(), getResolvedSchema(request, resolved),
                getResolvedSchema(response, resolved), getResolvedSchema(errors, resolved));
      }
      result.getMessages().put(entry.getKey(), nvalue);
    }
    Schemas.copyProperties(protocol, result);
    return result;
  }


  /**
   * Resolve all unresolved schema references.
   * @param schema - the schema to resolved references for.
   * @param protocol - the protocol we resolve the schema's for.
   * (we lookup all unresolved schema references in the protocol)
   * @param resolved - a map of all resolved schema's so far.
   * @return - a instance of the resolved schema.
   */
  static Schema resolve(final Schema schema, final Protocol protocol, final Map<String, Schema> resolved) {
    final String fullName = schema.getFullName();
    if (fullName != null && resolved.containsKey(fullName)) {
      return resolved.get(schema.getFullName());
    } else if (isUnresolvedSchema(schema)) {
      final String unresolvedSchemaName = getUnresolvedSchemaName(schema);
      Schema type = protocol.getType(unresolvedSchemaName);
      if (type == null) {
        throw new IllegalArgumentException("Cannot resolve " + unresolvedSchemaName);
      }
      return resolve(type, protocol, resolved);
    } else {
      switch (schema.getType()) {
        case RECORD:
          Schema createRecord = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(),
                  schema.isError());
          resolved.put(schema.getFullName(), createRecord);
          final List<Schema.Field> currFields = schema.getFields();
          List<Schema.Field> newFields = new ArrayList<Schema.Field>(currFields.size());
          for (Schema.Field field : currFields) {
            if (field.name().equals("hash")) {
              System.err.println(field);
            }
            Schema.Field nf = new Schema.Field(field.name(), resolve(field.schema(), protocol, resolved),
                    field.doc(), field.defaultVal(), field.order());
            Schemas.copyAliases(field, nf);
            Schemas.copyProperties(field, nf);
            newFields.add(nf);
          }
          createRecord.setFields(newFields);
          Schemas.copyLogicalTypes(schema, createRecord);
          Schemas.copyProperties(schema, createRecord);
          return createRecord;
        case MAP:
          Schema result = Schema.createMap(resolve(schema.getValueType(), protocol, resolved));
          Schemas.copyProperties(schema, result);
          return result;
        case ARRAY:
          Schema aresult = Schema.createArray(resolve(schema.getElementType(), protocol, resolved));
          Schemas.copyProperties(schema, aresult);
          return aresult;
        case UNION:
          final List<Schema> uTypes = schema.getTypes();
          List<Schema> newTypes = new ArrayList<Schema>(uTypes.size());
          for (Schema s : uTypes) {
            newTypes.add(resolve(s, protocol, resolved));
          }
          Schema bresult = Schema.createUnion(newTypes);
          Schemas.copyProperties(schema, bresult);
          return bresult;
        case ENUM:
        case FIXED:
        case STRING:
        case BYTES:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case NULL:
          return schema;
        default:
          throw new RuntimeException("Unknown type: " + schema);
      }
    }
  }

  /**
   * get the resolved schema.
   * @param schema - the schema we want to get the resolved equivalent for.
   * @param resolved - a Map wil all resolved schemas
   * @return - the resolved schema.
   */
  public static Schema getResolvedSchema(final Schema schema, final Map<String, Schema> resolved) {
    if (schema == null) {
      return null;
    }
    final String fullName = schema.getFullName();
    if (fullName != null && resolved.containsKey(fullName)) {
      return resolved.get(schema.getFullName());
    } else {
      switch (schema.getType()) {
        case RECORD:
          Schema createRecord = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(),
              schema.isError());
          resolved.put(schema.getFullName(), createRecord);
          final List<Schema.Field> currFields = schema.getFields();
          List<Schema.Field> newFields = new ArrayList<Schema.Field>(currFields.size());
          for (Schema.Field field : currFields) {
            if (field.name().equals("hash")) {
              System.err.println(field);
            }
            Schema.Field nf = new Schema.Field(field.name(), getResolvedSchema(field.schema(), resolved),
                    field.doc(), field.defaultVal(), field.order());
            Schemas.copyAliases(field, nf);
            Schemas.copyProperties(field, nf);
            newFields.add(nf);
          }
          createRecord.setFields(newFields);
          Schemas.copyLogicalTypes(schema, createRecord);
          Schemas.copyProperties(schema, createRecord);
          return createRecord;
        case MAP:
          Schema createMap = Schema.createMap(getResolvedSchema(schema.getValueType(), resolved));
          Schemas.copyProperties(schema, createMap);
          return createMap;
        case ARRAY:
          Schema createArray = Schema.createArray(getResolvedSchema(schema.getElementType(), resolved));
          Schemas.copyProperties(schema, createArray);
          return createArray;
        case UNION:
          final List<Schema> uTypes = schema.getTypes();
          List<Schema> newTypes = new ArrayList<Schema>(uTypes.size());
          for (Schema s : uTypes) {
            newTypes.add(getResolvedSchema(s, resolved));
          }
          Schema createUnion = Schema.createUnion(newTypes);
          Schemas.copyProperties(schema, createUnion);
          return createUnion;
        case ENUM:
        case FIXED:
        case STRING:
        case BYTES:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case NULL:
          return schema;
        default:
          throw new RuntimeException("Unknown type: " + schema);
      }
    }
  }

}
