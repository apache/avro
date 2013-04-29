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
package org.apache.avro;

import java.util.Collection;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.NullNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * A fluent interface for building {@link Schema} instances. Example usage:
 * </p>
 * <pre><code>Schema schema = SchemaBuilder
 *   .recordType("myrecord").namespace("org.example").aliases("oldrecord")
 *   .requiredString("f0")
 *   .requiredLong("f1").doc("This is f1")
 *   .optionalBoolean("f2", true)
 *   .build();
 </code></pre>
 */
public class SchemaBuilder {

  public static final Schema NULL = Schema.create(Schema.Type.NULL);
  public static final Schema BOOLEAN = Schema.create(Schema.Type.BOOLEAN);
  public static final Schema INT = Schema.create(Schema.Type.INT);
  public static final Schema LONG = Schema.create(Schema.Type.LONG);
  public static final Schema FLOAT = Schema.create(Schema.Type.FLOAT);
  public static final Schema DOUBLE = Schema.create(Schema.Type.DOUBLE);
  public static final Schema BYTES = Schema.create(Schema.Type.BYTES);
  public static final Schema STRING = Schema.create(Schema.Type.STRING);

  private SchemaBuilder() {
  }

  /**
   * Create a builder for an Avro record with the specified name.
   * @param name the record name
   */
  public static RecordBuilder recordType(String name) {
    return new RecordBuilder(name);
  }

  /**
   * Create a builder for an Avro error with the specified name.
   * @param name the error name
   */
  public static RecordBuilder errorType(String name) {
    return new RecordBuilder(name, true);
  }

  /**
   * Create a builder for a reference to an Avro record with the specified name; used
   * when constructing recursive schemas.
   * @param name the name of the referenced record
   */
  public static RecordReferenceBuilder recordReference(String name) {
    return new RecordReferenceBuilder(name);
  }

  /**
   * Create a builder for an Avro enum with the specified name and symbols (values).
   * @param name the enum name
   * @param values the symbols of the enum
   */
  public static EnumBuilder enumType(String name, String... values) {
    return new EnumBuilder(name, values);
  }

  /**
   * Create a builder for an Avro array with the specified schema for the array's items.
   * @param schema the schema for the array's items
   */
  public static ArrayBuilder arrayType(Schema schema) {
    return new ArrayBuilder(schema);
  }

  /**
   * Create a builder for an Avro map with the specified schema for the map's values.
   * @param schema the schema for the map's values
   */
  public static MapBuilder mapType(Schema schema) {
    return new MapBuilder(schema);
  }

  /**
   * Create a builder for an Avro fixed type with the specified name and size.
   * @param name the fixed name
   * @param size the the number of bytes per value
   */
  public static FixedBuilder fixedType(String name, int size) {
    return new FixedBuilder(name, size);
  }

  /**
   * Create a builder for an Avro union with the specified types.
   * @param types the types in the union
   */
  public static UnionBuilder unionType(Schema... types) {
    return new UnionBuilder(types);
  }

  public static class RecordReferenceBuilder {

    private final String name;
    private String namespace;

    RecordReferenceBuilder(String name) {
      checkRequired(name, "Record name is required.");
      this.name = name;
    }

    /**
     * Specify the optional namespace for this schema. If the name already specifies a
     * namespace then this call has no effect.
     * @param namespace a string qualifying the name
     */
    public RecordReferenceBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Build a record schema referencing another record schema.
     * @return a record schema
     */
    public Schema build() {
      return Schema.createRecord(name, null, namespace, false);
    }

  }

  abstract static class FieldBuilderBase {

    /**
     * Create a builder for a required boolean field with the specified name.
     * @param name the field name
     */
    public FieldBuilder requiredBoolean(String name) {
      return new FieldBuilder(this, name, BOOLEAN);
    }

    /**
     * Create a builder for an optional boolean field with the specified name and a
     * default value of null.
     * @param name the field name
     */
    public FieldBuilder optionalBoolean(String name) {
      return new FieldBuilder(this, name, BOOLEAN, true);
    }

    /**
     * Create a builder for an optional boolean field with the specified name and
     * default value.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalBoolean(String name, boolean defaultValue) {
      return new FieldBuilder(this, name, BOOLEAN, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required int field with the specified name.
     * @param name the field name
     */
    public FieldBuilder requiredInt(String name) {
      return new FieldBuilder(this, name, INT);
    }

    /**
     * Create a builder for an optional int field with the specified name and a
     * default value of null.
     * @param name the field name
     */
    public FieldBuilder optionalInt(String name) {
      return new FieldBuilder(this, name, INT, true);
    }

    /**
     * Create a builder for an optional int field with the specified name and
     * default value.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalInt(String name, int defaultValue) {
      return new FieldBuilder(this, name, INT, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required long field with the specified name.
     * @param name the field name
     */
    public FieldBuilder requiredLong(String name) {
      return new FieldBuilder(this, name, LONG);
    }

    /**
     * Create a builder for an optional long field with the specified name and a
     * default value of null.
     * @param name the field name
     */
    public FieldBuilder optionalLong(String name) {
      return new FieldBuilder(this, name, LONG, true);
    }

    /**
     * Create a builder for an optional long field with the specified name and
     * default value.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalLong(String name, long defaultValue) {
      return new FieldBuilder(this, name, LONG, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required float field with the specified name.
     * @param name the field name
     */
    public FieldBuilder requiredFloat(String name) {
      return new FieldBuilder(this, name, FLOAT);
    }

    /**
     * Create a builder for an optional float field with the specified name and a
     * default value of null.
     * @param name the field name
     */
    public FieldBuilder optionalFloat(String name) {
      return new FieldBuilder(this, name, FLOAT, true);
    }

    /**
     * Create a builder for an optional float field with the specified name and
     * default value.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalFloat(String name, float defaultValue) {
      return new FieldBuilder(this, name, FLOAT, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required double field with the specified name.
     * @param name the field name
     */
    public FieldBuilder requiredDouble(String name) {
      return new FieldBuilder(this, name, DOUBLE);
    }

    /**
     * Create a builder for an optional double field with the specified name and a
     * default value of null.
     * @param name the field name
     */
    public FieldBuilder optionalDouble(String name) {
      return new FieldBuilder(this, name, DOUBLE, true);
    }

    /**
     * Create a builder for an optional double field with the specified name and
     * default value.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalDouble(String name, double defaultValue) {
      return new FieldBuilder(this, name, DOUBLE, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required bytes field with the specified name.
     * @param name the field name
     */
    public FieldBuilder requiredBytes(String name) {
      return new FieldBuilder(this, name, BYTES);
    }

    /**
     * Create a builder for an optional bytes field with the specified name and a
     * default value of null.
     * @param name the field name
     */
    public FieldBuilder optionalBytes(String name) {
      return new FieldBuilder(this, name, BYTES, true);
    }

    /**
     * Create a builder for an optional bytes field with the specified name and
     * default value.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalBytes(String name, byte[] defaultValue) {
      return new FieldBuilder(this, name, BYTES, true,
          toJsonNode(ByteBuffer.wrap(defaultValue)));
    }

    /**
     * Create a builder for a required string field with the specified name.
     * @param name the field name
     */
    public FieldBuilder requiredString(String name) {
      return new FieldBuilder(this, name, STRING);
    }

    /**
     * Create a builder for an optional string field with the specified name and a
     * default value of null.
     * @param name the field name
     */
    public FieldBuilder optionalString(String name) {
      return new FieldBuilder(this, name, STRING, true);
    }

    /**
     * Create a builder for an optional string field with the specified name and
     * default value.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalString(String name, CharSequence defaultValue) {
      return new FieldBuilder(this, name, STRING, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required record with the specified name and type.
     * @param name the field name
     * @param schema the record type
     */
    public FieldBuilder requiredRecord(String name, Schema schema) {
      return new FieldBuilder(this, name, schema);
    }

    /**
     * Create a builder for an optional record with the specified name and type,
     * and a default value of null.
     * @param name the field name
     * @param schema the record type
     */
    public FieldBuilder optionalRecord(String name, Schema schema) {
      return new FieldBuilder(this, name, schema, true);
    }

    /**
     * Create a builder for an optional record with the specified name, type, and default
     * value.
     * @param name the field name
     * @param schema the record type
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalRecord(String name, Schema schema,
        GenericRecord defaultValue) {
      return new FieldBuilder(this, name, schema, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required enum with the specified name and type.
     * @param name the field name
     * @param schema the field type (record, enum, array, map, fixed, or union)
     */
    public FieldBuilder requiredEnum(String name, Schema schema) {
      return new FieldBuilder(this, name, schema);
    }

    /**
     * Create a builder for an optional enum with the specified name and type,
     * and a default value of null.
     * @param name the field name
     * @param schema the enum type
     */
    public FieldBuilder optionalEnum(String name, Schema schema) {
      return new FieldBuilder(this, name, schema, true);
    }

    /**
     * Create a builder for an optional enum with the specified name, type, and default
     * value.
     * @param name the field name
     * @param schema the enum type
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalEnum(String name, Schema schema,
        CharSequence defaultValue) {
      return new FieldBuilder(this, name, schema, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required array with the specified name and type.
     * @param name the field name
     * @param schema the array type
     */
    public FieldBuilder requiredArray(String name, Schema schema) {
      return new FieldBuilder(this, name, schema);
    }

    /**
     * Create a builder for an optional array with the specified name and type,
     * and a default value of null.
     * @param name the field name
     * @param schema the array type
     */
    public FieldBuilder optionalArray(String name, Schema schema) {
      return new FieldBuilder(this, name, schema, true);
    }

    /**
     * Create a builder for an optional array with the specified name, type, and default
     * value.
     * @param name the field name
     * @param schema the array type
     * @param defaultValue the default value of the field if unspecified
     */
    public <E> FieldBuilder optionalArray(String name, Schema schema,
        Collection<E> defaultValue) {
      return new FieldBuilder(this, name, schema, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required map with the specified name and type.
     * @param name the field name
     * @param schema the map type
     */
    public FieldBuilder requiredMap(String name, Schema schema) {
      return new FieldBuilder(this, name, schema);
    }

    /**
     * Create a builder for an optional map with the specified name and type,
     * and a default value of null.
     * @param name the field name
     * @param schema the map type
     */
    public FieldBuilder optionalMap(String name, Schema schema) {
      return new FieldBuilder(this, name, schema, true);
    }

    /**
     * Create a builder for an optional map with the specified name, type, and default
     * value.
     * @param name the field name
     * @param schema the map type
     * @param defaultValue the default value of the field if unspecified
     */
    public <K, V> FieldBuilder optionalMap(String name, Schema schema,
        Map<K, V> defaultValue) {
      return new FieldBuilder(this, name, schema, true, toJsonNode(defaultValue));
    }

    /**
     * Create a builder for a required fixed with the specified name and type.
     * @param name the field name
     * @param schema the fixed type
     */
    public FieldBuilder requiredFixed(String name, Schema schema) {
      return new FieldBuilder(this, name, schema);
    }

    /**
     * Create a builder for an optional fixed with the specified name and type,
     * and a default value of null.
     * @param name the field name
     * @param schema the fixed type
     */
    public FieldBuilder optionalFixed(String name, Schema schema) {
      return new FieldBuilder(this, name, schema, true);
    }

    /**
     * Create a builder for an optional fixed with the specified name, type, and default
     * value.
     * @param name the field name
     * @param schema the fixed type
     * @param defaultValue the default value of the field if unspecified
     */
    public FieldBuilder optionalFixed(String name, Schema schema,
        byte[] defaultValue) {
      return new FieldBuilder(this, name, schema, true,
          toJsonNode(ByteBuffer.wrap(defaultValue)));
    }

    /**
     * Create a builder for a union with the specified name and type.
     * @param name the field name
     * @param schema the union type
     */
    public FieldBuilder unionType(String name, Schema schema) {
      return new FieldBuilder(this, name, schema);
    }

    /**
     * Create a builder for a union of a boolean with the default value and further
     * types.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionBoolean(String name, boolean defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, BOOLEAN, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a int with the default value and further types.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionInt(String name, int defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, INT, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a long with the default value and further types.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionLong(String name, long defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, LONG, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a float with the default value and further types.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionFloat(String name, float defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, FLOAT, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a double with the default value and further types.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionDouble(String name, double defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, DOUBLE, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a bytes with the default value and further types.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionBytes(String name, byte[] defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, BYTES,
          toJsonNode(ByteBuffer.wrap(defaultValue)), remainingTypes);
    }

    /**
     * Create a builder for a union of a string with the default value and further types.
     * @param name the field name
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionString(String name, CharSequence defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, STRING, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a record with the default value and further types.
     * @param name the field name
     * @param type the record type
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionRecord(String name, Schema type, GenericRecord defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, type, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of an enum with the default value and further types.
     * @param name the field name
     * @param type the enum type
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionEnum(String name, Schema type, CharSequence defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, type, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of an array with the default value and further types.
     * @param name the field name
     * @param type the array type
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public <E> FieldBuilder unionArray(String name, Schema type,
        Collection<E> defaultValue, Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, type, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a map with the default value and further types.
     * @param name the field name
     * @param type the map type
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public <K, V> FieldBuilder unionMap(String name, Schema type, Map<K, V> defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, type, toJsonNode(defaultValue),
          remainingTypes);
    }

    /**
     * Create a builder for a union of a fixed with the default value and further types.
     * @param name the field name
     * @param type the fixed type
     * @param defaultValue the default value of the field if unspecified
     * @param remainingTypes an array or varargs of one or more types in the union
     */
    public FieldBuilder unionFixed(String name, Schema type, byte[] defaultValue,
        Schema... remainingTypes) {
      return new UnionFieldBuilder(this, name, type,
          toJsonNode(ByteBuffer.wrap(defaultValue)), remainingTypes);
    }

    protected JsonNode toJsonNode(Object o) {
      try {
        String s;
        if (o instanceof ByteBuffer) {
          // special case since GenericData.toString() is incorrect for bytes
          // note that this does not handle the case of a default value with nested bytes
          ByteBuffer bytes = ((ByteBuffer) o);
          StringBuilder buffer = new StringBuilder();
          buffer.append('"');
          for (int i = bytes.position(); i < bytes.limit(); i++)
            buffer.append((char)bytes.get(i));
          buffer.append('"');
          s = buffer.toString();
        } else {
          s = GenericData.get().toString(o);
        }
        return new ObjectMapper().readTree(s);
      } catch (IOException e) {
        throw new SchemaBuilderException(e);
      }
    }

    abstract Schema getRecordSchema();

    abstract List<Schema.Field> updateAndGetFields();
  }

  public static class RecordBuilder extends FieldBuilderBase {

    private final String name;
    private final boolean error;
    private String namespace;
    private String doc;
    private Set<String> aliases = new HashSet<String>();

    RecordBuilder(String name) {
      this(name, false);
    }

    RecordBuilder(String name, boolean error) {
      checkRequired(name, "Record name is required.");
      this.name = name;
      this.error = error;
    }

    /**
     * Specify the optional namespace for this schema. If the name already specifies a
     * namespace then this call has no effect.
     * @param namespace a string qualifying the name
     */
    public RecordBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Specify the optional documentation string for this schema.
     * @param doc a documentation string
     */
    public RecordBuilder doc(String doc) {
      this.doc = doc;
      return this;
    }

    /**
     * Specify one or more optional aliases providing alternate names for this schema.
     * @param aliases an array or varargs of one or more string names
     */
    public RecordBuilder aliases(String... aliases) {
      for (String alias : aliases) {
        this.aliases.add(alias);
      }
      return this;
    }

    Schema getRecordSchema() {
      Schema schema = Schema.createRecord(name, doc, namespace, error);
      for (String alias : aliases) {
        schema.addAlias(alias);
      }
      return schema;
    }

    @Override
    List<Schema.Field> updateAndGetFields() {
      return new ArrayList<Schema.Field>(); // no previous fields
    }
  }

  public static class FieldBuilder extends FieldBuilderBase {

    private final Schema recordSchema;
    private final List<Schema.Field> fields;
    private final String name;
    private final Schema fieldSchema;
    private final boolean optional;
    private JsonNode defaultValue;
    private String doc;
    private Schema.Field.Order order;
    private Set<String> aliases = new HashSet<String>();

    FieldBuilder(FieldBuilderBase builder, String name, Schema fieldSchema) {
      this(builder, name, fieldSchema, false, null); // no default specified
    }

    FieldBuilder(FieldBuilderBase builder, String name, Schema fieldSchema,
        JsonNode defaultValue) {
      this(builder, name, fieldSchema, false, defaultValue);
    }

    FieldBuilder(FieldBuilderBase builder, String name, Schema fieldSchema,
        boolean optional) {
      this(builder, name, fieldSchema, optional, NullNode.getInstance()); // null default
    }

    FieldBuilder(FieldBuilderBase builder, String name, Schema fieldSchema,
        boolean optional, JsonNode defaultValue) {
      checkRequired(name, "Field name is required.");
      checkRequired(fieldSchema, "Field schema is required for %s.", name);

      this.recordSchema = builder.getRecordSchema();
      this.fields = builder.updateAndGetFields();
      this.name = name;
      this.fieldSchema = fieldSchema;
      this.optional = optional;
      this.defaultValue = defaultValue;
    }

    /**
     * Specify the optional documentation string for this field.
     * @param doc a documentation string
     */
    public FieldBuilder doc(String doc) {
      this.doc = doc;
      return this;
    }

    /**
     * Specify that this field should use the value's natural sort order. This is the
     * default if no sort order is specified.
     */
    public FieldBuilder orderAscending() {
      this.order = Schema.Field.Order.ASCENDING;
      return this;
    }

    /**
     * Specify that this field should use the value's reverse sort order.
     */
    public FieldBuilder orderDescending() {
      this.order = Schema.Field.Order.DESCENDING;
      return this;
    }

    /**
     * Specify that this field should be ignored when sorting records.
     */
    public FieldBuilder orderIgnore() {
      this.order = Schema.Field.Order.IGNORE;
      return this;
    }

    /**
     * Specify one or more optional aliases providing alternate names for this field.
     * @param aliases an array or varargs of one or more string names
     */
    public FieldBuilder aliases(String... aliases) {
      for (String alias : aliases) {
        this.aliases.add(alias);
      }
      return this;
    }

    /**
     * <b>Expert</b>. Specify the default value for this field. In most cases, the
     * <code>requiredX()</code> and <code>optionalX()</code> methods suffice,
     * but this method can be used to create a non-nullable (required) field with a
     * default value.
     * @param defaultValue
     */
    public FieldBuilder defaultValue(Object defaultValue) {
      this.defaultValue = toJsonNode(defaultValue);
      return this;
    }

    @Override
    Schema getRecordSchema() {
      return recordSchema;
    }

    @Override
    List<Schema.Field> updateAndGetFields() {
      Schema s;
      if (optional) {
        List<Schema> types = new ArrayList<Schema>();
        if (NullNode.getInstance().equals(defaultValue)) {
          types.add(Schema.create(Schema.Type.NULL));
          types.add(fieldSchema);
        } else {
          types.add(fieldSchema);
          types.add(Schema.create(Schema.Type.NULL));
        }
        s = Schema.createUnion(types);
      } else {
        s = fieldSchema;
      }
      Schema.Field field;
      if (order == null) {
        field = new Schema.Field(name, s, doc, defaultValue);
      } else {
        field = new Schema.Field(name, s, doc, defaultValue, order);
      }
      for (String alias : aliases) {
        field.addAlias(alias);
      }
      fields.add(field);
      return fields;
    }

    /**
     * Build a record schema with the fields specified.
     * @return a record schema
     */
    public Schema build() {
      recordSchema.setFields(updateAndGetFields());
      return recordSchema;
    }

  }

  public static class UnionFieldBuilder extends FieldBuilder {
    public UnionFieldBuilder(FieldBuilderBase builder, String name, Schema firstType,
        Schema... remainingTypes) {
      super(builder, name, union(firstType, remainingTypes));
    }

    public UnionFieldBuilder(FieldBuilderBase builder, String name, Schema firstType,
        JsonNode defaultValue, Schema... remainingTypes) {
      super(builder, name, union(firstType, remainingTypes), defaultValue);
    }

    private static Schema union(Schema firstType, Schema... remainingTypes) {
      List<Schema> types = new ArrayList<Schema>();
      types.add(firstType);
      types.addAll(Arrays.asList(remainingTypes));
      return Schema.createUnion(types);
    }
  }

  public static class EnumBuilder {

    private final String name;
    private final List<String> values;
    private String namespace;
    private String doc;
    private Set<String> aliases = new HashSet<String>();

    EnumBuilder(String name, String... values) {
      checkRequired(name, "Enum name is required.");
      checkRequired(values, "Enum values are required for %s.", name);

      this.name = name;
      this.values = Arrays.asList(values);
    }

    /**
     * Specify the optional namespace for this schema. If the name already specifies a
     * namespace then this call has no effect.
     * @param namespace a string qualifying the name
     */
    public EnumBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Specify the optional documentation string for this schema.
     * @param doc a documentation string
     */
    public EnumBuilder doc(String doc) {
      this.doc = doc;
      return this;
    }

    /**
     * Specify one or more optional aliases providing alternate names for this schema.
     * @param aliases an array or varargs of one or more string names
     */
    public EnumBuilder aliases(String... aliases) {
      for (String alias : aliases) {
        this.aliases.add(alias);
      }
      return this;
    }

    /**
     * Build an enum schema.
     * @return an enum schema
     */
    public Schema build() {
      Schema schema = Schema.createEnum(name, doc, namespace, values);
      for (String alias : aliases) {
        schema.addAlias(alias);
      }
      return schema;
    }

  }

  public static class ArrayBuilder {

    private final Schema itemsSchema;

    ArrayBuilder(Schema itemsSchema) {
      checkRequired(itemsSchema, "Array items schema is required.");
      this.itemsSchema = itemsSchema;
    }

    /**
     * Build an array schema.
     * @return aa array schema
     */
    public Schema build() {
      return Schema.createArray(itemsSchema);
    }

  }

  public static class MapBuilder {

    private final Schema valuesSchema;

    MapBuilder(Schema valuesSchema) {
      checkRequired(valuesSchema, "Map values schema is required.");
      this.valuesSchema = valuesSchema;
    }

    /**
     * Build a map schema.
     * @return a map schema
     */
    public Schema build() {
      return Schema.createMap(valuesSchema);
    }

  }

  public static class FixedBuilder {

    private final String name;
    private final Integer size;
    private String namespace;
    private String doc;
    private Set<String> aliases = new HashSet<String>();

    FixedBuilder(String name, int size) {
      checkRequired(name, "Fixed name is required.");
      checkRequired(size, "Fixed size is required for %s.", name);
      this.name = name;
      this.size = size;
    }

    /**
     * Specify the optional namespace for this schema. If the name already specifies a
     * namespace then this call has no effect.
     * @param namespace a string qualifying the name
     */
    public FixedBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Specify the optional documentation string for this schema.
     * @param doc a documentation string
     */
    public FixedBuilder doc(String doc) {
      this.doc = doc;
      return this;
    }

    /**
     * Specify one or more optional aliases providing alternate names for this schema.
     * @param aliases an array or varargs of one or more string names
     */
    public FixedBuilder aliases(String... aliases) {
      for (String alias : aliases) {
        this.aliases.add(alias);
      }
      return this;
    }

    /**
     * Build a fixed schema.
     * @return a fixed schema
     */
    public Schema build() {
      Schema schema = Schema.createFixed(name, doc, namespace, size);
      for (String alias : aliases) {
        schema.addAlias(alias);
      }
      return schema;
    }

  }

  public static class UnionBuilder {

    private final List<Schema> types;

    UnionBuilder(Schema... types) {
      this(Arrays.asList(types));
    }

    UnionBuilder(List<Schema> types) {
      checkRequired(types, "Union types are required.");
      this.types = types;
    }

    /**
     * Build a union schema.
     * @return a union schema
     */
    public Schema build() {
      return Schema.createUnion(types);
    }

  }

  private static void checkRequired(Object reference, String errorMessage) {
    if (reference == null) {
      throw new NullPointerException(errorMessage);
    }
  }

  private static void checkRequired(Object reference, String errorMessage,
      Object... errorMessageArgs) {
    if (reference == null) {
      throw new NullPointerException(String.format(errorMessage, errorMessageArgs));
    }
  }
}
