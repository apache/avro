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
package org.apache.avro.compiler.specific;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedSchema;

/**
 *
 * @author zoly
 */
public final class SchemaResolver {

    private SchemaResolver() { }

    public static Protocol resolve(final Protocol protocol) {
        Protocol result = new Protocol(protocol.getName(), protocol.getDoc(), protocol.getNamespace());
        final Collection<Schema> types = protocol.getTypes();
        List<Schema> newSchemas = new ArrayList(types.size());
        Map<String, Schema> processed =  new HashMap<String, Schema>();
        for (Schema schema : types) {
            newSchemas.add(resolve(schema, protocol, processed));
        }
        result.setTypes(newSchemas);

        for (Map.Entry<String, Protocol.Message> entry : protocol.getMessages().entrySet()) {
            Protocol.Message value = entry.getValue();
            Protocol.Message nvalue;
            if (value.isOneWay()) {
                Schema request = value.getRequest();
                nvalue = result.createMessage(value.getName(), value.getDoc(),
                        value.getObjectProps(), intern(request, processed));
            } else {
                Schema request = value.getRequest();
                Schema response = value.getResponse();
                Schema errors = value.getErrors();
                nvalue = result.createMessage(value.getName(), value.getDoc(),
                        value.getObjectProps(), intern(request, processed),
                        intern(response, processed), intern(errors, processed));
            }
            result.getMessages().put(entry.getKey(), nvalue);
        }
        result.addJsonProps(protocol.getJsonProps());
        return result;
    }

    public static Schema resolve(final Schema schema, final Protocol protocol, final Map<String, Schema> processed) {
        final String fullName = schema.getFullName();
        if (fullName != null && processed.containsKey(fullName)) {
            return processed.get(schema.getFullName());
        } else if (schema instanceof UnresolvedSchema) {
            Schema type = protocol.getType(schema.getFullName());
            if (type == null) {
                throw new IllegalArgumentException("Cannot resolve " + schema);
            }
            return resolve(type, protocol, processed);
        } else {
            switch (schema.getType()) {
                case RECORD:
                    Schema createRecord = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(),
                    schema.isError());
                    processed.put(schema.getFullName(), createRecord);
                    final List<Schema.Field> currFields = schema.getFields();
                    List<Schema.Field> newFields = new ArrayList<Schema.Field>(currFields.size());
                    for (Schema.Field field : currFields) {
                        Schema.Field nf = new Schema.Field(field.name(), resolve(field.schema(), protocol, processed),
                                field.doc(), field.defaultVal(), field.order());
                        for (String alias : field.aliases()) {
                            nf.addAlias(alias);
                        }
                        newFields.add(nf);
                    }
                    createRecord.setFields(newFields);
                    if (schema.getLogicalType() != null) {
                        createRecord.setLogicalType(schema.getLogicalType());
                    }
                    createRecord.addJsonProps(schema.getJsonProps());
                    return createRecord;
                case MAP:
                    return Schema.createMap(resolve(schema.getValueType(), protocol, processed));
                case ARRAY:
                    return Schema.createArray(resolve(schema.getElementType(), protocol, processed));
                case UNION:
                    final List<Schema> uTypes = schema.getTypes();
                    List<Schema> newTypes = new ArrayList<Schema>(uTypes.size());
                    for (Schema s : uTypes) {
                        newTypes.add(resolve(s, protocol, processed));
                    }
                    return Schema.createUnion(newTypes);
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

    public static Schema intern(final Schema schema, final Map<String, Schema> processed) {
        if (schema == null) {
            return null;
        }
        final String fullName = schema.getFullName();
        if (fullName != null && processed.containsKey(fullName)) {
            return processed.get(schema.getFullName());
        } else {
            switch (schema.getType()) {
                case RECORD:
                    Schema createRecord = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(),
                    schema.isError());
                    processed.put(schema.getFullName(), createRecord);
                    final List<Schema.Field> currFields = schema.getFields();
                    List<Schema.Field> newFields = new ArrayList<Schema.Field>(currFields.size());
                    for (Schema.Field field : currFields) {
                        Schema.Field nf = new Schema.Field(field.name(), intern(field.schema(), processed),
                                field.doc(), field.defaultVal(), field.order());
                        for (String alias : field.aliases()) {
                            nf.addAlias(alias);
                        }
                        newFields.add(nf);
                    }
                    createRecord.setFields(newFields);
                    if (schema.getLogicalType() != null) {
                        createRecord.setLogicalType(schema.getLogicalType());
                    }
                    createRecord.addJsonProps(schema.getJsonProps());
                    return createRecord;
                case MAP:
                    return Schema.createMap(intern(schema.getValueType(), processed));
                case ARRAY:
                    return Schema.createArray(intern(schema.getElementType(), processed));
                case UNION:
                    final List<Schema> uTypes = schema.getTypes();
                    List<Schema> newTypes = new ArrayList<Schema>(uTypes.size());
                    for (Schema s : uTypes) {
                        newTypes.add(intern(s, processed));
                    }
                    return Schema.createUnion(newTypes);
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
