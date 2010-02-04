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
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Iterator;
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
    FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
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
  Map<String,String> props = new HashMap<String,String>(1);

  Schema(Type type) { this.type = type; }

  /** Create a schema for a primitive type. */
  public static Schema create(Type type) {
    switch (type) {
    case STRING:  return new StringSchema();
    case BYTES:   return new BytesSchema();
    case INT:     return new IntSchema();
    case LONG:    return new LongSchema();
    case FLOAT:   return new FloatSchema();
    case DOUBLE:  return new DoubleSchema();
    case BOOLEAN: return new BooleanSchema();
    case NULL:    return new NullSchema();
    default: throw new AvroRuntimeException("Can't create a: "+type);
    }
  }

  private static final Set<String> RESERVED_PROPS = new HashSet<String>();
  static {
    Collections.addAll(RESERVED_PROPS,
                       "fields", "items", "name", "namespace",
                       "size", "symbols", "values", "type");
  }

  /** Return the value of the named property in this schema. */
  public synchronized String getProp(String name) {
    return props.get(name);
  }

  /** Set the value of the named property in this schema. */
  public synchronized void setProp(String name, String value) {
    if (RESERVED_PROPS.contains(name))
      throw new AvroRuntimeException("Can't set a reserved property: "+name);
    if (value == null)
      props.remove(name);
    else
      props.put(name, value);
  }

  /** Create an anonymous record schema. */
  public static Schema createRecord(LinkedHashMap<String,Field> fields) {
    Schema result = createRecord(null, null, null, false);
    result.setFields(fields);
    return result;
  }

  /** Create a named record schema. */
  public static Schema createRecord(String name, String doc, String namespace,
                                    boolean isError) {
    return new RecordSchema(new Name(name, namespace), doc, isError);
  }

  /** Create an enum schema. */
  public static Schema createEnum(String name, String doc, String namespace,
                                  List<String> values) {
    return new EnumSchema(new Name(name, namespace), doc, values);
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
  public static Schema createFixed(String name, String doc, String space,
      int size) {
    return new FixedSchema(new Name(name, space), doc, size);
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
  
  /** If this is an enum, returns true if it contains given symbol. */
  public boolean hasEnumSymbol(String symbol) {
    throw new AvroRuntimeException("Not an enum: "+this);
  }

  /** If this is a record, enum or fixed, returns its name, otherwise the name
   * of the primitive type. */
  public String getName() { return type.name; }

  /** If this is a record, enum, or fixed, returns its docstring,
   * if available.  Otherwise, returns null. */
  public String getDoc() {
    return null;
  }

  /** If this is a record, enum or fixed, returns its namespace, if any. */
  public String getNamespace() {
    throw new AvroRuntimeException("Not a named type: "+this);
  }

  /** If this is a record, enum or fixed, returns its namespace-qualified name,
   * if any. */
  public String getFullName() {
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
  @Override
  public String toString() { return toString(false); }

  /** Render this as <a href="http://json.org/">JSON</a>.
   * @param pretty if true, pretty-print JSON.
   */
  public String toString(boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = FACTORY.createJsonGenerator(writer);
      if (pretty) gen.useDefaultPrettyPrinter();
      toJson(new Names(), gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  void toJson(Names names, JsonGenerator gen) throws IOException {
    if (props.size() == 0) {                      // no props defined
      gen.writeString(getName());                 // just write name
    } else {
      gen.writeStartObject();
      gen.writeStringField("type", getName());
      writeProps(gen);
      gen.writeEndObject();
    }
  }

  void writeProps(JsonGenerator gen) throws IOException {
    for (Map.Entry<String,String> e : props.entrySet())
      gen.writeStringField(e.getKey(), e.getValue());
  }

  void fieldsToJson(Names names, JsonGenerator gen) throws IOException {
    throw new AvroRuntimeException("Not a record: "+this);
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Schema)) return false;
    Schema that = (Schema)o;
    if (!(this.type == that.type)) return false;
    return props.equals(that.props);
  }
  public int hashCode() { return getType().hashCode() + props.hashCode(); }

  /** A field within a record. */
  public static class Field {

    /** How values of this field should be ordered when sorting records. */
    public enum Order {
      ASCENDING, DESCENDING, IGNORE;
      private String name;
      private Order() { this.name = this.name().toLowerCase(); }
    };

    private final String name;    // name of the field.
    private int position = -1;
    private final Schema schema;
    private final String doc;
    private final JsonNode defaultValue;
    private final Order order;

    public Field(String name, Schema schema, String doc,
        JsonNode defaultValue) {
      this(name, schema, doc, defaultValue, Order.ASCENDING);
    }
    public Field(String name, Schema schema, String doc,
        JsonNode defaultValue, Order order) {
      this.name = name;
      this.schema = schema;
      this.doc = doc;
      this.defaultValue = defaultValue;
      this.order = order;
    }
    public String name() { return name; };
    /** The position of this field within the record. */
    public int pos() { return position; }
    /** This field's {@link Schema}. */
    public Schema schema() { return schema; }
    /** Field's documentation within the record, if set. May return null. */
    public String doc() { return doc; }
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
    private final String name;
    private final String space;
    private final String full;
    public Name(String name, String space) {
      if (name == null) {                         // anonymous
        this.name = this.space = this.full = null;
        return;
      }
      int lastDot = name.lastIndexOf('.');
      if (lastDot < 0) {                          // unqualified name
        this.space = space;                       // use default space
        this.name = name;
      } else {                                    // qualified name
        this.space = name.substring(0, lastDot);  // get space from name
        this.name = name.substring(lastDot+1, name.length());
      }
      this.full = (this.space == null) ? this.name : this.space+"."+this.name;
    }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Name)) return false;
      Name that = (Name)o;
      return full==null ? that.full==null : full.equals(that.full);
    }
    public int hashCode() {
      return full==null ? 0 : full.hashCode();
    }
    public String toString() { return full; }
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
    private final String doc;
    public NamedSchema(Type type, Name name, String doc) {
      super(type);
      this.name = name;
      this.doc = doc;
      if (PRIMITIVES.containsKey(name.full)) {
        throw new AvroTypeException("Schemas may not be named after primitives: " + name.full);
      }
    }
    public String getName() { return name.name; }
    public String getDoc() { return doc; }
    public String getNamespace() { return name.space; }
    public String getFullName() { return name.full; }
    public boolean writeNameRef(Names names, JsonGenerator gen)
      throws IOException {
      if (this.equals(names.get(name))) {
        if (name.space == null || name.space.equals(names.space()))
          gen.writeString(name.name);             // in default namespace
        else
          gen.writeString(name.full);             // use fully-qualified name
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
    public int hashCode() {
      return getType().hashCode() + name.hashCode() + props.hashCode();
    }
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
    public RecordSchema(Name name, String doc, boolean isError) {
      super(Type.RECORD, name, doc);
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
      if (!props.equals(that.props)) return false;
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
      writeProps(gen);
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
    public EnumSchema(Name name, String doc, List<String> symbols) {
      super(Type.ENUM, name, doc);
      this.symbols = symbols;
      this.ordinals = new HashMap<String,Integer>();
      int i = 0;
      for (String symbol : symbols)
        if (ordinals.put(symbol, i++) != null)
          throw new SchemaParseException("Duplicate enum symbol: "+symbol);
    }
    public List<String> getEnumSymbols() { return symbols; }
    public boolean hasEnumSymbol(String symbol) { 
      return ordinals.containsKey(symbol); }
    public int getEnumOrdinal(String symbol) { return ordinals.get(symbol); }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof EnumSchema)) return false;
      EnumSchema that = (EnumSchema)o;
      return equalNames(that)
        && symbols.equals(that.symbols)
        && props.equals(that.props);
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
      writeProps(gen);
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
      if (!(o instanceof ArraySchema)) return false;
      ArraySchema that = (ArraySchema)o;
      return elementType.equals(that.elementType) && props.equals(that.props);
    }
    public int hashCode() {
      return getType().hashCode() + elementType.hashCode() + props.hashCode();
    }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", "array");
      gen.writeFieldName("items");
      elementType.toJson(names, gen);
      writeProps(gen);
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
      if (!(o instanceof MapSchema)) return false;
      MapSchema that = (MapSchema)o;
      return valueType.equals(that.valueType) && props.equals(that.props);
    }
    public int hashCode() {
      return getType().hashCode() + valueType.hashCode() + props.hashCode();
    }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", "map");
      gen.writeFieldName("values");
      valueType.toJson(names, gen);
      writeProps(gen);
      gen.writeEndObject();
    }
  }

  private static class UnionSchema extends Schema {
    private final List<Schema> types;
    public UnionSchema(List<Schema> types) {
      super(Type.UNION);
      this.types = types;
      int seen = 0;
      Set<String> seenNames = new HashSet<String>();
      for (Schema type : types) {                 // check legality of union
        switch (type.getType()) {
        case UNION: 
          throw new AvroRuntimeException("Nested union: "+this);
        case RECORD:
        case FIXED:
        case ENUM:
          String fullname = type.getFullName();
          if (fullname != null) {
            if (seenNames.add(fullname)) {
              continue;
            } else {
              throw new AvroRuntimeException("Duplicate name in union:" + fullname);
            }
          } else {
            throw new AvroRuntimeException("Nameless Record, Fixed, or Enum in union:"+this);
          }
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
      if (!(o instanceof UnionSchema)) return false;
      UnionSchema that = (UnionSchema)o;
      return types.equals(that.types) && props.equals(that.props);
    }
    public int hashCode() {
      return getType().hashCode() + types.hashCode() + props.hashCode();
    }
    public void setProp(String name, String value) {
      throw new AvroRuntimeException("Can't set properties on a union: "+this);
    }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      gen.writeStartArray();
      for (Schema type : types)
        type.toJson(names, gen);
      gen.writeEndArray();
    }
  }

  private static class FixedSchema extends NamedSchema {
    private final int size;
    public FixedSchema(Name name, String doc, int size) {
      super(Type.FIXED, name, doc);
      if (size < 0)
        throw new IllegalArgumentException("Invalid fixed size: "+size);
      this.size = size;
    }
    public int getFixedSize() { return size; }
    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof FixedSchema)) return false;
      FixedSchema that = (FixedSchema)o;
      return equalNames(that) && size == that.size && props.equals(that.props);
    }
    public int hashCode() { return super.hashCode() + size; }
    void toJson(Names names, JsonGenerator gen) throws IOException {
      if (writeNameRef(names, gen)) return;
      gen.writeStartObject();
      gen.writeStringField("type", "fixed");
      writeName(names, gen);
      gen.writeNumberField("size", size);
      writeProps(gen);
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

  /**
   * Constructs a Schema object from JSON schema file <tt>file</tt>.
   * The contents of <tt>file</tt> is expected to be in UTF-8 format.
   * @param file  The file to read the schema from.
   * @return  The freshly built Schema.
   * @throws IOException if there was trouble reading the contents
   * @throws JsonParseException if the contents are invalid
   */
  public static Schema parse(File file) throws IOException {
    JsonParser parser = FACTORY.createJsonParser(file);
    try {
      return Schema.parse(MAPPER.readTree(parser), new Names());
    } catch (JsonParseException e) {
      throw new SchemaParseException(e);
    }
  }

  /**
   * Constructs a Schema object from JSON schema stream <tt>in</tt>.
   * The contents of <tt>in</tt> is expected to be in UTF-8 format.
   * @param in  The input stream to read the schema from.
   * @return  The freshly built Schema.
   * @throws IOException if there was trouble reading the contents
   * @throws JsonParseException if the contents are invalid
   */
  public static Schema parse(InputStream in) throws IOException {
    JsonParser parser = FACTORY.createJsonParser(in);
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

  static final Map<String,Type> PRIMITIVES = new HashMap<String,Type>();
  static {
    PRIMITIVES.put("string",  Type.STRING);
    PRIMITIVES.put("bytes",   Type.BYTES);
    PRIMITIVES.put("int",     Type.INT);
    PRIMITIVES.put("long",    Type.LONG);
    PRIMITIVES.put("float",   Type.FLOAT);
    PRIMITIVES.put("double",  Type.DOUBLE);
    PRIMITIVES.put("boolean", Type.BOOLEAN);
    PRIMITIVES.put("null",    Type.NULL);
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
        Type primitive = PRIMITIVES.get((String)o);
        if (primitive != null) return Schema.create(primitive);
        name = new Name((String)o, space);
      } else {
        name = (Name)o;
      }
      return super.get(name);
    }
    public boolean contains(Schema schema) {
      return get(((NamedSchema)schema).name) != null;
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
  }

  /** @see #parse(String) */
  static Schema parse(JsonNode schema, Names names) {
    if (schema.isTextual()) {                     // name
      Schema result = names.get(schema.getTextValue());
      if (result == null)
        throw new SchemaParseException("Undefined name: "+schema);
      return result;
    } else if (schema.isObject()) {
      Schema result;
      String type = getRequiredText(schema, "type", "No type");
      Name name = null;
      String savedSpace = null;
      String doc = null;
      if (type.equals("record") || type.equals("error")
          || type.equals("enum") || type.equals("fixed")) {
        String space = getOptionalText(schema, "namespace");
        doc = getOptionalText(schema, "doc");
        if (space == null)
          space = names.space();
        name = new Name(getRequiredText(schema, "name", "No name in schema"),
                        space);
        if (name.space != null) {                 // set default namespace
          savedSpace = names.space();
          names.space(name.space);
        }
      }
      if (PRIMITIVES.containsKey(type)) {         // primitive
        result = create(PRIMITIVES.get(type));
      } else if (type.equals("record") || type.equals("error")) { // record
        LinkedHashMap<String,Field> fields = new LinkedHashMap<String,Field>();
        result = new RecordSchema(name, doc, type.equals("error"));
        if (name != null) names.add(result);
        JsonNode fieldsNode = schema.get("fields");
        if (fieldsNode == null || !fieldsNode.isArray())
          throw new SchemaParseException("Record has no fields: "+schema);
        for (JsonNode field : fieldsNode) {
          String fieldName = getRequiredText(field, "name", "No field name");
          String fieldDoc = getOptionalText(field, "doc");
          JsonNode fieldTypeNode = field.get("type");
          if (fieldTypeNode == null)
            throw new SchemaParseException("No field type: "+field);
          Schema fieldSchema = parse(fieldTypeNode, names);
          Field.Order order = Field.Order.ASCENDING;
          JsonNode orderNode = field.get("order");
          if (orderNode != null)
            order = Field.Order.valueOf(orderNode.getTextValue().toUpperCase());
          fields.put(fieldName, new Field(fieldName, fieldSchema,
              fieldDoc, field.get("default"), order));
        }
        result.setFields(fields);
      } else if (type.equals("enum")) {           // enum
        JsonNode symbolsNode = schema.get("symbols");
        if (symbolsNode == null || !symbolsNode.isArray())
          throw new SchemaParseException("Enum has no symbols: "+schema);
        List<String> symbols = new ArrayList<String>();
        for (JsonNode n : symbolsNode)
          symbols.add(n.getTextValue());
        result = new EnumSchema(name, doc, symbols);
        if (name != null) names.add(result);
      } else if (type.equals("array")) {          // array
        JsonNode itemsNode = schema.get("items");
        if (itemsNode == null)
          throw new SchemaParseException("Array has no items type: "+schema);
        result = new ArraySchema(parse(itemsNode, names));
      } else if (type.equals("map")) {            // map
        JsonNode valuesNode = schema.get("values");
        if (valuesNode == null)
          throw new SchemaParseException("Map has no values type: "+schema);
        result = new MapSchema(parse(valuesNode, names));
      } else if (type.equals("fixed")) {          // fixed
        JsonNode sizeNode = schema.get("size");
        if (sizeNode == null || !sizeNode.isInt())
          throw new SchemaParseException("Invalid or no size: "+schema);
        result = new FixedSchema(name, doc, sizeNode.getIntValue());
        if (name != null) names.add(result);
      } else
        throw new SchemaParseException("Type not supported: "+type);
      Iterator<String> i = schema.getFieldNames();
      while (i.hasNext()) {                       // add properties
        String prop = i.next();
        if (!RESERVED_PROPS.contains(prop))       // ignore reserved
          result.setProp(prop, schema.get(prop).getTextValue());
      }
      if (savedSpace != null)
        names.space(savedSpace);                  // restore space
      return result;
    } else if (schema.isArray()) {                // union
      List<Schema> types = new ArrayList<Schema>(schema.size());
      for (JsonNode typeNode : schema)
        types.add(parse(typeNode, names));
      return new UnionSchema(types);
    } else {
      throw new SchemaParseException("Schema not yet supported: "+schema);
    }
  }

  /** Extracts text value associated to key from the container JsonNode,
   * and throws {@link SchemaParseException} if it doesn't exist.
   *
   * @param container Container where to find key.
   * @param key Key to look for in container.
   * @param error String to prepend to the SchemaParseException.
   * @return
   */
  private static String getRequiredText(JsonNode container, String key,
      String error) {
    String out = getOptionalText(container, key);
    if (null == out) {
      throw new SchemaParseException(error + ": " + container);
    }
    return out;
  }

  /** Extracts text value associated to key from the container JsonNode. */
  private static String getOptionalText(JsonNode container, String key) {
    JsonNode jsonNode = container.get(key);
    return jsonNode != null ? jsonNode.getTextValue() : null;
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
