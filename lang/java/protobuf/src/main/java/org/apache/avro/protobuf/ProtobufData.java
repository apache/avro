/**
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
package org.apache.avro.protobuf;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.io.IOException;
import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileOptions;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;

/** Utilities for serializing Protobuf data in Avro format. */
public class ProtobufData extends GenericData {
  private static final String PROTOBUF_TYPE = "protobuf";

  private static final ProtobufData INSTANCE = new ProtobufData();

  protected ProtobufData() {}
  
  /** Return the singleton instance. */
  public static ProtobufData get() { return INSTANCE; }

  @Override
  public DatumReader createDatumReader(Schema schema) {
    return new ProtobufDatumReader(schema, schema, this);
  }

  @Override
  public DatumWriter createDatumWriter(Schema schema) {
    return new ProtobufDatumWriter(schema, this);
  }

  @Override
  public void setField(Object r, String n, int pos, Object o) {
    setField(r, n, pos, o, getRecordState(r, getSchema(r.getClass())));
  }

  @Override
  public Object getField(Object r, String name, int pos) {
    return getField(r, name, pos, getRecordState(r, getSchema(r.getClass())));
  }

  @Override
  protected void setField(Object r, String n, int pos, Object o, Object state) {
    Builder b = (Builder)r;
    FieldDescriptor f = ((FieldDescriptor[])state)[pos];
    switch (f.getType()) {
    case MESSAGE:
      if (o == null) {
        b.clearField(f);
        break;
      }
    default:
      b.setField(f, o);
    }
  }

  @Override
  protected Object getField(Object record, String name, int pos, Object state) {
    Message m = (Message)record;
    FieldDescriptor f = ((FieldDescriptor[])state)[pos];
    switch (f.getType()) {
    case MESSAGE:
      if (!f.isRepeated() && !m.hasField(f))
        return null;
    default:
      return m.getField(f);
    }
  }    

  private final Map<Descriptor,FieldDescriptor[]> fieldCache =
    new ConcurrentHashMap<Descriptor,FieldDescriptor[]>();

  @Override
  protected Object getRecordState(Object r, Schema s) {
    Descriptor d = ((MessageOrBuilder)r).getDescriptorForType();
    FieldDescriptor[] fields = fieldCache.get(d);
    if (fields == null) {                         // cache miss
      fields = new FieldDescriptor[s.getFields().size()];
      for (Field f : s.getFields())
        fields[f.pos()] = d.findFieldByName(f.name());
      fieldCache.put(d, fields);                  // update cache
    }
    return fields;
  }

  @Override
  protected boolean isRecord(Object datum) {
    return datum instanceof Message;
  }

  @Override
  public Object newRecord(Object old, Schema schema) {
    try {
      Class c = Class.forName(SpecificData.getClassName(schema));
      if (c == null)
        return newRecord(old, schema);            // punt to generic
      if (c.isInstance(old))
        return old;                               // reuse instance
      return c.getMethod("newBuilder").invoke(null);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean isArray(Object datum) {
    return datum instanceof List;
  }

  @Override
  protected boolean isBytes(Object datum) {
    return datum instanceof ByteString;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    return getSchema(((Message)record).getDescriptorForType());
  }

  private final Map<Class,Schema> schemaCache
    = new ConcurrentHashMap<Class,Schema>();

  /** Return a record schema given a protobuf message class. */
  public Schema getSchema(Class c) {
    Schema schema = schemaCache.get(c);

    if (schema == null) {                         // cache miss
      try {
        Object descriptor = c.getMethod("getDescriptor").invoke(null);
        if (c.isEnum())
          schema = getSchema((EnumDescriptor)descriptor);
        else
          schema = getSchema((Descriptor)descriptor);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      schemaCache.put(c, schema);                 // update cache
    }
    return schema;
  }

  private static final ThreadLocal<Map<Descriptor,Schema>> SEEN
    = new ThreadLocal<Map<Descriptor,Schema>>() {
    protected Map<Descriptor,Schema> initialValue() {
      return new IdentityHashMap<Descriptor,Schema>();
    }
  };

  private Schema getSchema(Descriptor descriptor) {
    Map<Descriptor,Schema> seen = SEEN.get();
    if (seen.containsKey(descriptor))             // stop recursion
      return seen.get(descriptor);
    boolean first = seen.isEmpty();
    try {
      Schema result =
        Schema.createRecord(descriptor.getName(), null,
                            getNamespace(descriptor.getFile(),
                                         descriptor.getContainingType()),
                            false);

      seen.put(descriptor, result);
        
      List<Field> fields = new ArrayList<Field>();
      for (FieldDescriptor f : descriptor.getFields())
        fields.add(new Field(f.getName(), getSchema(f), null, getDefault(f)));
      result.setFields(fields);
      return result;

    } finally {
      if (first)
        seen.clear();
    }
  }

  private String getNamespace(FileDescriptor fd, Descriptor containing) {
    FileOptions o = fd.getOptions();
    String p = o.hasJavaPackage()
      ? o.getJavaPackage()
      : fd.getPackage();
    String outer;
    if (o.hasJavaOuterClassname()) {
      outer = o.getJavaOuterClassname();
    } else {
      outer = new File(fd.getName()).getName();
      outer = outer.substring(0, outer.lastIndexOf('.'));
      outer = toCamelCase(outer);
    }
    String inner = "";
    while (containing != null) {
      inner = containing.getName() + "$" + inner;
      containing = containing.getContainingType();
    }
    return p + "." + outer + "$" + inner;
  }

  private static String toCamelCase(String s){
    String[] parts = s.split("_");
    String camelCaseString = "";
    for (String part : parts) {
      camelCaseString = camelCaseString + cap(part);
    }
    return camelCaseString;
  }

  private static String cap(String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
  }

  private static final Schema NULL = Schema.create(Schema.Type.NULL);

  private Schema getSchema(FieldDescriptor f) {
    Schema s = getNonRepeatedSchema(f);
    if (f.isRepeated())
      s = Schema.createArray(s);
    return s;
  }

  private Schema getNonRepeatedSchema(FieldDescriptor f) {
    Schema result;
    switch (f.getType()) {
    case BOOL:
      return Schema.create(Schema.Type.BOOLEAN);
    case FLOAT:
      return Schema.create(Schema.Type.FLOAT);
    case DOUBLE:
      return Schema.create(Schema.Type.DOUBLE);
    case STRING:
      Schema s = Schema.create(Schema.Type.STRING);
      GenericData.setStringType(s, GenericData.StringType.String);
      return s;
    case BYTES:
      return Schema.create(Schema.Type.BYTES);
    case INT32: case UINT32: case SINT32: case FIXED32: case SFIXED32:
      return Schema.create(Schema.Type.INT);
    case INT64: case UINT64: case SINT64: case FIXED64: case SFIXED64:
      return Schema.create(Schema.Type.LONG);
    case ENUM:
      return getSchema(f.getEnumType());
    case MESSAGE:
      result = getSchema(f.getMessageType());
      if (f.isOptional())
        // wrap optional record fields in a union with null
        result = Schema.createUnion(Arrays.asList(new Schema[] {NULL, result}));
      return result;
    case GROUP:                                   // groups are deprecated
    default:
      throw new RuntimeException("Unexpected type: "+f.getType());
    }
  }

  private Schema getSchema(EnumDescriptor d) {
    List<String> symbols = new ArrayList<String>();
    for (EnumValueDescriptor e : d.getValues()) {
      symbols.add(e.getName());
    }
    return Schema.createEnum(d.getName(), null,
                             getNamespace(d.getFile(), d.getContainingType()),
                             symbols);
  }

  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private JsonNode getDefault(FieldDescriptor f) {
    if (f.isRequired() || f.isRepeated())         // no default
      return null;

    if (f.hasDefaultValue()) {                    // parse spec'd default value
      String json = toString(f.getDefaultValue());
      try {
        return MAPPER.readTree(FACTORY.createJsonParser(json));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    switch (f.getType()) {                        // generate default for type
    case BOOL:
      return NODES.booleanNode(false);
    case FLOAT: case DOUBLE:
    case INT32: case UINT32: case SINT32: case FIXED32: case SFIXED32:
    case INT64: case UINT64: case SINT64: case FIXED64: case SFIXED64:
      return NODES.numberNode(0);
    case STRING: case BYTES:
      return NODES.textNode("");
    case ENUM:
      return NODES.textNode(f.getEnumType().getValues().get(0).getName());
    case MESSAGE:
      return NODES.nullNode();
    case GROUP:                                   // groups are deprecated
    default:
      throw new RuntimeException("Unexpected type: "+f.getType());
    }
    
  }

}
