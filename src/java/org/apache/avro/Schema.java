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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

/** An abstract data type.
 * <p>A schema may be one of:
 * <ul>
 * <li>A <i>record</i>, mapping field names to field value data;
 * <li>An <i>enum</i>, containing one of a small set of symbols;
 * <li>An <i>array</i> of values, all of the same schema;
 * <li>A <i>map</i>, containing string/value pairs, of a declared schema;
 * <li>A <i>union</i> of other schemas;
 * <li>A <i>fixed</i> sized binary object;
 * <li>A unicode <i>string</i>;
 * <li>A sequence of <i>bytes</i>;
 * <li>A 32-bit signed <i>int</i>;
 * <li>A 64-bit signed <i>long</i>;
 * <li>A 32-bit IEEE single-<i>float</i>; or
 * <li>A 64-bit IEEE <i>double</i>-float; or
 * <li>A <i>boolean</i>; or
 * <li><i>null</i>.
 * </ul>
 */
public abstract class Schema {
  static final JsonFactory FACTORY = new JsonFactory();
  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  static {
    FACTORY.enableParserFeature(JsonParser.Feature.ALLOW_COMMENTS);
    FACTORY.setCodec(MAPPER);
  }

  /** The type of a schema. */
  public enum Type {
    RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES,
      INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;
    private String name;
    private Type() { this.name = this.name().toLowerCase(); }
  };

  private final Type type;

  Schema(Type type) { this.type = type; }

  /** Create a schema for a primitive type. */
  public static Schema create(Type type) {
    switch (type) {
    case STRING:  return STRING_SCHEMA;
    case BYTES:   return BYTES_SCHEMA;
    case INT:     return INT_SCHEMA ;
    case LONG:    return LONG_SCHEMA ;
    case FLOAT:   return FLOAT_SCHEMA;
    case DOUBLE:  return DOUBLE_SCHEMA;
    case BOOLEAN: return BOOLEAN_SCHEMA;
    case NULL:    return NULL_SCHEMA;
    default: throw new AvroRuntimeException("Can't create a: "+type);
    }
  }

  /** Create an anonymous record schema. */
  public static Schema createRecord(LinkedHashMap<String,Field> fields) {
    Schema result = createRecord(null, null, false);
    result.setFields(fields);
    return result;
  }

  /** Create a named record schema. */
  public static Schema createRecord(String name, String namespace,
                                    boolean isError) {
     return new RecordSchema(name, namespace, isError);
  }

  /** Create an enum schema. */
  public static Schema createEnum(String name, String namespace,
                                  List<String> values) {
    return new EnumSchema(name, namespace, values);
  }

  /** Create an array schema. */
  public static Schema createArray(Schema elementType) {
    return new ArraySchema(elementType);
  }

  /** Create a map schema. */
  public static Schema createMap(Schema valueType) {
    return new MapSchema(valueType);
  }

  /** Create a union schema. */
  public static Schema createUnion(List<Schema> types) {
    return new UnionSchema(types);
  }

  /** Create a union schema. */
  public static Schema createFixed(String name, String space, int size) {
    return new FixedSchema(name, space, size);
  }

  /** Return the type of this schema. */
  public Type getType() { return type; }

  /** If this is a record, returns its fields. */
  public Map<String, Field> getFields() {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  /** If this is a record, enumerate its field names and their schemas. */
  public Iterable<Map.Entry<String,Schema>> getFieldSchemas() {
    throw new AvroRuntimeException("Not a record: "+this);
  }
  
  /** If this is a record, set its fields. */
  public void setFields(LinkedHashMap<String,Field> fields) {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  /** If this is an enum, return its symbols. */
  public List<String> getEnumSymbols() {
    throw new AvroRuntimeException("Not an enum: "+this);
  }    

  /** If this is an enum, return a symbol's ordinal value. */
  public int getEnumOrdinal(String symbol) {
    throw new AvroRuntimeException("Not an enum: "+this);
  }    

  /** If this is a record, enum or fixed, returns its name, otherwise the name
   * of the primitive type. */
  public String getName() { return type.name; }

  /** If this is a record, enum or fixed, returns its namespace, if any. */
  public String getNamespace() {
    throw new AvroRuntimeException("Not a named type: "+this);
  }

  /** Returns true if this record is an error type. */
  public boolean isError() {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  /** If this is an array, returns its element type. */
  public Schema getElementType() {
    throw new AvroRuntimeException("Not an array: "+this);
  }

  /** If this is a map, returns its value type. */
  public Schema getValueType() {
    throw new AvroRuntimeException("Not a map: "+this);
  }

  /** If this is a union, returns its types. */
  public List<Schema> getTypes() {
    throw new AvroRuntimeException("Not a union: "+this);
  }

  /** If this is fixed, returns its size. */
  public int getFixedSize() {
    throw new AvroRuntimeException("Not fixed: "+this);
  }

  /** Render this as <a href="http://json.org/">JSON</a>.*/
  public String toString() {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = FACTORY.createJsonGenerator(writer);
      toJson(new Names(), gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  void toJson(Names names, JsonGenerator gen) throws IOException {
    gen.writeString(getName());
  }

  void fieldsToJson(Names names, JsonGenerator gen) throws IOException {
    throw new AvroRuntimeException("Not a record: "+this);
  }


  public boolean equals(Object o) {
    if (o == this) return true;
    return o instanceof Schema && type.equals(((Schema)o).type);
  }
  public int hashCode() { return getType().hashCode(); }

  /** A field within a record. */
  public static class Field {

    /** How values of this field should be ordered when sorting records. */
    public enum Order {
      ASCENDING, DESCENDING, IGNORE;
      private String name;
      private Order() { this.name = this.name().toLowerCase(); }
    };

    private int position = -1;
    private final Schema schema;
    private final JsonNode defaultValue;
    private final Order order;

    public Field(Schema schema, JsonNode defaultValue) {
      this(schema, defaultValue, Order.ASCENDING);
    }
    public Field(Schema schema, JsonNode defaultValue, Order order) {
      this.schema = schema;
      this.defaultValue = defaultValue;
      this.order = order;
    }
    /** The position of this field within the record. */
    public int pos() { return position; }
    /** This field's {@link Schema}. */
    public Schema schema() { return schema; }
    public JsonNode defaultValue() { return defaultValue; }
    public Order order() { return order; }
    public boolean equals(Object other) {
      if (other == this) return true;
      if (!(other instanceof Field)) return false;
      Field that = (Field) other;
      return (position == that.position) &&
        (schema.equals(that.schema)) &&
        (defaultValue == null
         ? that.defaultValue == null
         : (defaultValue.equals(that.defaultValue)));
    }
    public int hashCode() { return schema.hashCode(); }
  }

  private static class Name {
    private String name;
    private String space;
    public Name(String name, String space) {
      if (name == null) return;                   // anonymous
      int lastDot = name.lastIndexOf('.');
      if (lastDot < 0) {                          // unqualified name
        this.space = space;                       // use default space
        this.name = name;
      } else {                                    // qualified name
        this.space = name.substring(0, lastDot);  // get space from name
        this.name = name.substring(lastDot+1, name.length());
      }
    }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Name)) return false;
      Name that = (Name)o;
      return that == null ? false
        : (name==null ? that.name==null : name.equals(that.name))
        && (space==null ? that.space==null : space.equals(that.space));
    }
    public int hashCode() {
      return (name==null ? 0 : name.hashCode())
        + (space==null ? 0 : space.hashCode());
    }
    public String toString() { return "name="+name + " namespace="+space; }
    public void writeName(Names names, JsonGenerator gen) throws IOException {
      if (name != null) gen.writeStringField("name", name);
      if (space != null) {
        if (!space.equals(names.space()))
          gen.writeStringField("namespace", space);
        if (names.space() == null)                // default namespace
          names.space(space);
      }
    }
  }

  private static abstract class NamedSchema extends Schema {
    private final Name name;
    public NamedSchema(Type type, String name, String space) {
      super(type);
      this.name = new Name(name, space);
    }
    public String getName() { return name.name; }
    public String getNamespace() { return name.space; }
    public boolean writeNameRef(Names names, JsonGenerator gen)
      throws IOException {
      if (this.equals(names.get(name))) {
        if (name.space == null || name.space.equals(names.space()))
          gen.writeString(name.name);
        else {
          gen.writeString(name.space+"."+name.name);
        }
        return true;
      } else if (name.name != null) {
        names.put(name, this);
      }
      return false;
    }
    public void writeName(Names names, JsonGenerator gen) throws IOException {
      name.writeName(names, gen);
    }
    public boolean equalNames(NamedSchema that) {
      return this.name.equals(that.name);
    }
    public int hashCode() { return getType().hashCode() + name.hashCode(); }
  }

  private static class SeenPair {
    private Object s1; private Object s2;
    private SeenPair(Object s1, Object s2) { this.s1 = s1; this.s2 = s2; }
    public boolean equals(Object o) {
      return this.s1 == ((SeenPair)o).s1 && this.s2 == ((SeenPair)o).s2;
    }
    public int hashCode() {
      return System.identityHashCode(s1) + System.identityHashCode(s2);
    }
  }

  private static final ThreadLocal<Set> SEEN_EQUALS = new ThreadLocal<Set>() {
    protected Set initialValue() { return new HashSet(); }
  };
  private static final ThreadLocal<Map> SEEN_HASHCODE = new ThreadLocal<Map>() {
    protected Map initialValue() { return new IdentityHashMap(); }
  };

  @SuppressWarnings(value="unchecked")
  private static class RecordSchema extends NamedSchema {
    private Map<String,Field> fields;
    private Iterable<Map.Entry<String,Schema>> fieldSchemas;
    private final boolean isError;
    public RecordSchema(String name, String space, boolean isError) {
      super(Type.RECORD, name, space);
      this.isError = isError;
    }
    public boolean isError() { return isError; }
    public Map<String, Field> getFields() { return fields; }
    public Iterable<Map.Entry<String, Schema>> getFieldSchemas() {
      return fieldSchemas;
    }
    public void setFields(LinkedHashMap<String,Field> fields) {
      if (this.fields != null)
        throw new AvroRuntimeException("Fields are already set");
      int i = 0;
      LinkedHashMap<String,Schema> schemas = new LinkedHashMap<String,Schema>();
      for (Map.Entry<String, Field> pair : fields.entrySet()) {
        Field f = pair.getValue();
        if (f.position != -1)
          throw new AvroRuntimeException("Field already used: "+f);
        f.position = i++;
        schemas.put(pair.getKey(), f.schema());
      }
      this.fields = fields;
      this.fieldSchemas = schemas.entrySet();
    }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof RecordSchema)) return false;
      RecordSchema that = (RecordSchema)o;
      if (!equalNames(that)) return false;
      if (!(o instanceof RecordSchema)) return false;
      Set seen = SEEN_EQUALS.get();
      SeenPair here = new SeenPair(this, o);
      if (seen.contains(here)) return true;       // prevent stack overflow
      try {
        seen.add(here);
        return fields.equals(((RecordSchema)o).fields);
      } finally {
        seen.remove(here);
      }
    }
    public int hashCode() {
      Map seen = SEEN_HASHCODE.get();
      if (seen.containsKey(this)) return 0;       // prevent stack overflow
      try {
        seen.put(this, this);
        return super.hashCode() + fields.hashCode();
      } finally {
        seen.remove(this);
      }
    }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      if (writeNameRef(names, gen)) return;
      gen.writeStartObject();
      gen.writeStringField("type", isError?"error":"record");
      writeName(names, gen);
      gen.writeFieldName("fields");
      fieldsToJson(names, gen);
      gen.writeEndObject();
    }

    void fieldsToJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartArray();
      for (Map.Entry<String, Field> entry : fields.entrySet()) {
        gen.writeStartObject();
        gen.writeStringField("name", entry.getKey());
        gen.writeFieldName("type");
        entry.getValue().schema().toJson(names, gen);
        if (entry.getValue().defaultValue() != null) {
          gen.writeFieldName("default");
          gen.writeTree(entry.getValue().defaultValue());
        }
        if (entry.getValue().order() != Field.Order.ASCENDING)
          gen.writeStringField("order", entry.getValue().order().name);
        gen.writeEndObject();
      }
      gen.writeEndArray();
    }
  }

  private static class EnumSchema extends NamedSchema {
    private final List<String> symbols;
    private final Map<String,Integer> ordinals;
    public EnumSchema(String name, String space, List<String> symbols) {
      super(Type.ENUM, name, space);
      this.symbols = symbols;
      this.ordinals = new HashMap<String,Integer>();
      int i = 0;
      for (String symbol : symbols)
        ordinals.put(symbol, i++);
    }
    public List<String> getEnumSymbols() { return symbols; }
    public int getEnumOrdinal(String symbol) { return ordinals.get(symbol); }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof EnumSchema)) return false;
      EnumSchema that = (EnumSchema)o;
      return equalNames(that) && symbols.equals(that.symbols);
    }
    public int hashCode() { return super.hashCode() + symbols.hashCode(); }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      if (writeNameRef(names, gen)) return;
      gen.writeStartObject();
      gen.writeStringField("type", "enum");
      writeName(names, gen);
      gen.writeArrayFieldStart("symbols");
      for (String symbol : symbols)
        gen.writeString(symbol);
      gen.writeEndArray();
      gen.writeEndObject();
    }
  }

  private static class ArraySchema extends Schema {
    private final Schema elementType;
    public ArraySchema(Schema elementType) {
      super(Type.ARRAY);
      this.elementType = elementType;
    }
    public Schema getElementType() { return elementType; }
    public boolean equals(Object o) {
      if (o == this) return true;
      return o instanceof ArraySchema
        && elementType.equals(((ArraySchema)o).elementType);
    }
    public int hashCode() {return getType().hashCode()+elementType.hashCode();}
    void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", "array");
      gen.writeFieldName("items");
      elementType.toJson(names, gen);
      gen.writeEndObject();
    }
  }

  private static class MapSchema extends Schema {
    private final Schema valueType;
    public MapSchema(Schema valueType) {
      super(Type.MAP);
      this.valueType = valueType;
    }
    public Schema getValueType() { return valueType; }
    public boolean equals(Object o) {
      if (o == this) return true;
      return o instanceof MapSchema
        && valueType.equals(((MapSchema)o).valueType);
    }
    public int hashCode() {
      return getType().hashCode()+valueType.hashCode();
    }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", "map");
      gen.writeFieldName("values");
      valueType.toJson(names, gen);
      gen.writeEndObject();
    }
  }

  private static class UnionSchema extends Schema {
    private final List<Schema> types;
    public UnionSchema(List<Schema> types) {
      super(Type.UNION);
      this.types = types;
      int seen = 0;
      for (Schema type : types) {                 // check legality of union
        switch (type.getType()) {
        case UNION: 
          throw new AvroRuntimeException("Nested union: "+this);
        case RECORD:
          if (type.getName() != null)
            continue;
        default:
          int mask = 1 << type.getType().ordinal();
          if ((seen & mask) != 0)
            throw new AvroRuntimeException("Ambiguous union: "+this);
          seen |= mask;
        }
      }
    }
    public List<Schema> getTypes() { return types; }
    public boolean equals(Object o) {
      if (o == this) return true;
      return o instanceof UnionSchema && types.equals(((UnionSchema)o).types);
    }
    public int hashCode() {return getType().hashCode()+types.hashCode();}
    void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartArray();
      for (Schema type : types)
        type.toJson(names, gen);
      gen.writeEndArray();
    }
  }

  private static class FixedSchema extends NamedSchema {
    private final int size;
    public FixedSchema(String name, String space, int size) {
      super(Type.FIXED, name, space);
      this.size = size;
    }
    public int getFixedSize() { return size; }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof FixedSchema)) return false;
      FixedSchema that = (FixedSchema)o;
      return equalNames(that) && size == that.size;
    }
    public int hashCode() { return super.hashCode() + size; }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      if (writeNameRef(names, gen)) return;
      gen.writeStartObject();
      gen.writeStringField("type", "fixed");
      writeName(names, gen);
      gen.writeNumberField("size", size);
      gen.writeEndObject();
    }
  }

  private static class StringSchema extends Schema {
    public StringSchema() { super(Type.STRING); }
  }

  private static class BytesSchema extends Schema {
    public BytesSchema() { super(Type.BYTES); }
  }

  private static class IntSchema extends Schema {
    public IntSchema() { super(Type.INT); }
  }

  private static class LongSchema extends Schema {
    public LongSchema() { super(Type.LONG); }
  }

  private static class FloatSchema extends Schema {
    public FloatSchema() { super(Type.FLOAT); }
  }

  private static class DoubleSchema extends Schema {
    public DoubleSchema() { super(Type.DOUBLE); }
  }

  private static class BooleanSchema extends Schema {
    public BooleanSchema() { super(Type.BOOLEAN); }
  }
  
  private static class NullSchema extends Schema {
    public NullSchema() { super(Type.NULL); }
  }
  
  private static final StringSchema  STRING_SCHEMA =  new StringSchema();
  private static final BytesSchema   BYTES_SCHEMA =   new BytesSchema();
  private static final IntSchema     INT_SCHEMA =     new IntSchema();
  private static final LongSchema    LONG_SCHEMA =    new LongSchema();
  private static final FloatSchema   FLOAT_SCHEMA =   new FloatSchema();
  private static final DoubleSchema  DOUBLE_SCHEMA =  new DoubleSchema();
  private static final BooleanSchema BOOLEAN_SCHEMA = new BooleanSchema();
  private static final NullSchema    NULL_SCHEMA =    new NullSchema();

  public static Schema parse(File file) throws IOException {
    JsonParser parser = FACTORY.createJsonParser(file);
    try {
      return Schema.parse(MAPPER.readTree(parser), new Names());
    } catch (JsonParseException e) {
      throw new SchemaParseException(e);
    }
  }

  /** Construct a schema from <a href="http://json.org/">JSON</a> text. */
  public static Schema parse(String jsonSchema) {
    return parse(parseJson(jsonSchema), new Names());
  }

  static final Map<String,Schema> PRIMITIVES = new HashMap<String,Schema>();
  static {
    PRIMITIVES.put("string",  STRING_SCHEMA);
    PRIMITIVES.put("bytes",   BYTES_SCHEMA);
    PRIMITIVES.put("int",     INT_SCHEMA);
    PRIMITIVES.put("long",    LONG_SCHEMA);
    PRIMITIVES.put("float",   FLOAT_SCHEMA);
    PRIMITIVES.put("double",  DOUBLE_SCHEMA);
    PRIMITIVES.put("boolean", BOOLEAN_SCHEMA);
    PRIMITIVES.put("null",    NULL_SCHEMA);
  }

  static class Names extends LinkedHashMap<Name, Schema> {
    private String space;                         // default namespace

    public Names() {}
    public Names(String space) { this.space = space; }

    public String space() { return space; }
    public void space(String space) { this.space = space; }

    @Override
    public Schema get(Object o) {
      Name name;
      if (o instanceof String) {
        Schema primitive = PRIMITIVES.get((String)o);
        if (primitive != null) return primitive;
        name = new Name((String)o, space);
      } else {
        name = (Name)o;
      }
      return super.get(name);
    }
    public void add(Schema schema) {
      put(((NamedSchema)schema).name, schema);
    }
    @Override
    public Schema put(Name name, Schema schema) {
      if (containsKey(name))
        throw new SchemaParseException("Can't redefine: "+name);
      return super.put(name, schema);
    }
    public Names except(final Schema schema) {
      final Names parent = this;
      return new Names(space) {
        public Schema get(Object o) {
          if (this.containsKey(o)) return this.get(o);
          if (((NamedSchema)schema).name.equals(o)) return null;
          return parent.get(o);
        }
      };
    }
  }

  /** @see #parse(String) */
  static Schema parse(JsonNode schema, Names names) {
    if (schema.isTextual()) {                     // name
      Schema result = names.get(schema.getTextValue());
      if (result == null)
        throw new SchemaParseException("Undefined name: "+schema);
      return result;
    } else if (schema.isObject()) {
      JsonNode typeNode = schema.get("type");
      if (typeNode == null)
        throw new SchemaParseException("No type: "+schema);
      String type = typeNode.getTextValue();
      String name = null, space = null;
      if (type.equals("record") || type.equals("error")
          || type.equals("enum") || type.equals("fixed")) {
        JsonNode nameNode = schema.get("name");
        name = nameNode != null ? nameNode.getTextValue() : null;
        JsonNode spaceNode = schema.get("namespace");
        space = spaceNode!=null?spaceNode.getTextValue():names.space();
        if (names.space() == null && space != null)
          names.space(space);                     // set default namespace
        if (name == null)
          throw new SchemaParseException("No name in schema: "+schema);
      }
      if (type.equals("record") || type.equals("error")) { // record
        LinkedHashMap<String,Field> fields = new LinkedHashMap<String,Field>();
        RecordSchema result =
          new RecordSchema(name, space, type.equals("error"));
        if (name != null) names.add(result);
        JsonNode fieldsNode = schema.get("fields");
        if (fieldsNode == null || !fieldsNode.isArray())
          throw new SchemaParseException("Record has no fields: "+schema);
        for (JsonNode field : fieldsNode) {
          JsonNode fieldNameNode = field.get("name");
          if (fieldNameNode == null)
            throw new SchemaParseException("No field name: "+field);
          JsonNode fieldTypeNode = field.get("type");
          if (fieldTypeNode == null)
            throw new SchemaParseException("No field type: "+field);
          Schema fieldSchema = parse(fieldTypeNode, names);
          Field.Order order = Field.Order.ASCENDING;
          JsonNode orderNode = field.get("order");
          if (orderNode != null)
            order = Field.Order.valueOf(orderNode.getTextValue().toUpperCase());
          fields.put(fieldNameNode.getTextValue(),
                     new Field(fieldSchema, field.get("default"), order));
        }
        result.setFields(fields);
        return result;
      } else if (type.equals("enum")) {           // enum
        JsonNode symbolsNode = schema.get("symbols");
        if (symbolsNode == null || !symbolsNode.isArray())
          throw new SchemaParseException("Enum has no symbols: "+schema);
        List<String> symbols = new ArrayList<String>();
        for (JsonNode n : symbolsNode)
          symbols.add(n.getTextValue());
        Schema result = new EnumSchema(name, space, symbols);
        if (name != null) names.add(result);
        return result;
      } else if (type.equals("array")) {          // array
        return new ArraySchema(parse(schema.get("items"), names));
      } else if (type.equals("map")) {            // map
        return new MapSchema(parse(schema.get("values"), names));
      } else if (type.equals("fixed")) {          // fixed
        Schema result = new FixedSchema(name, space,
                                        schema.get("size").getIntValue());
        if (name != null) names.add(result);
        return result;
      } else
        throw new SchemaParseException("Type not yet supported: "+type);
    } else if (schema.isArray()) {                // union
      List<Schema> types = new ArrayList<Schema>(schema.size());
      for (JsonNode typeNode : schema)
        types.add(parse(typeNode, names));
      return new UnionSchema(types);
    } else {
      throw new SchemaParseException("Schema not yet supported: "+schema);
    }
  }

  static JsonNode parseJson(String s) {
    try {
      return MAPPER.readTree(FACTORY.createJsonParser(new StringReader(s)));
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
