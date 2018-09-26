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
package org.apache.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.internal.JacksonUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.io.JsonStringEncoder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.TextNode;

/**
 * <p>
 * A fluent interface for building {@link Schema} instances. The flow of the API
 * is designed to mimic the <a
 * href="http://avro.apache.org/docs/current/spec.html#schemas">Avro Schema
 * Specification</a>
 * </p>
 * For example, the below JSON schema and the fluent builder code to create it
 * are very similar:
 *
 * <pre>
 * {
 *   "type": "record",
 *   "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
 *   "fields": [
 *     {"name": "clientHash",
 *      "type": {"type": "fixed", "name": "MD5", "size": 16}},
 *     {"name": "clientProtocol", "type": ["null", "string"]},
 *     {"name": "serverHash", "type": "MD5"},
 *     {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
 *   ]
 * }
 * </pre>
 *
 * <pre>
 *   Schema schema = SchemaBuilder
 *   .record("HandshakeRequest").namespace("org.apache.avro.ipc")
 *   .fields()
 *     .name("clientHash").type().fixed("MD5").size(16).noDefault()
 *     .name("clientProtocol").type().nullable().stringType().noDefault()
 *     .name("serverHash").type("MD5")
 *     .name("meta").type().nullable().map().values().bytesType().noDefault()
 *   .endRecord();
 * </pre>
 * <br>
 *
 * <h1>Usage Guide</h1>
 * SchemaBuilder chains together many smaller builders and maintains nested
 * context in order to mimic the Avro Schema specification. Every Avro type in
 * JSON has required and optional JSON properties, as well as user-defined
 * properties.
 * <br>
 * <h2>Selecting and Building an Avro Type</h2>
 * The API analogy for the right hand side of the Avro Schema JSON
 * <pre>
 * "type":
 * </pre>
 * is a {@link TypeBuilder}, {@link FieldTypeBuilder}, or
 * {@link UnionFieldTypeBuilder}, depending on the context. These types all
 * share a similar API for selecting and building types.
 * <br>
 * <h1>Primitive Types</h1>
 * All Avro primitive types are trivial to configure. A primitive type in
 * Avro JSON can be declared two ways, one that supports custom properties
 * and one that does not:
 * <pre>
 * {"type":"int"}
 * {"type":{"name":"int"}}
 * {"type":{"name":"int", "customProp":"val"}}
 * </pre>
 * The analogous code form for the above three JSON lines are the below
 * three lines:
 * <pre>
 *  .intType()
 *  .intBuilder().endInt()
 *  .intBuilder().prop("customProp", "val").endInt()
 * </pre>
 * Every primitive type has a shortcut to create the trivial type, and
 * a builder when custom properties are required.  The first line above is
 * a shortcut for the second, analogous to the JSON case.
 * <h2>Named Types</h2>
 * Avro named types have names, namespace, aliases, and doc.  In this API
 * these share a common parent, {@link NamespacedBuilder}.
 * The builders for named types require a name to be constructed, and optional
 * configuration via:
 * <ul>
 * <li>{@link NamespacedBuilder#doc()}</li>
 * <li>{@link NamespacedBuilder#namespace(String)}</li>
 * <li>{@link NamespacedBuilder#aliases(String...)}</li>
 * <li>{@link PropBuilder#prop(String, String)}</li>
 * </ul>
 * <br>
 * Each named type completes configuration of the optional properties
 * with its own method:
 * <ul>
 * <li>{@link FixedBuilder#size(int)}</li>
 * <li>{@link EnumBuilder#symbols(String...)}</li>
 * <li>{@link RecordBuilder#fields()}</li>
 * </ul>
 * Example use of a named type with all optional parameters:
 * <pre>
 * .enumeration("Suit").namespace("org.apache.test")
 *   .aliases("org.apache.test.OldSuit")
 *   .doc("CardSuits")
 *   .prop("customProp", "val")
 *   .symbols("SPADES", "HEARTS", "DIAMONDS", "CLUBS")
 * </pre>
 * Which is equivalent to the JSON:
 * <pre>
 * { "type":"enum",
 *   "name":"Suit", "namespace":"org.apache.test",
 *   "aliases":["org.apache.test.OldSuit"],
 *   "doc":"Card Suits",
 *   "customProp":"val",
 *   "symbols":["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
 * }
 * </pre>
 * <h2>Nested Types</h2>
 * The Avro nested types, map and array, can have custom properties like
 * all avro types, are not named, and must specify a nested type.
 * After configuration of optional properties, an array or map
 * builds or selects its nested type with {@link ArrayBuilder#items()}
 * and {@link MapBuilder#values()}, respectively.
 *
 * <h2>Fields</h2>
 * {@link RecordBuilder#fields()} returns a {@link FieldAssembler} for
 * defining the fields of the record and completing it.
 * Each field must have a name, specified via {@link FieldAssembler#name(String)},
 * which returns a {@link FieldBuilder} for defining aliases, custom properties,
 * and documentation of the field.  After configuring these optional values for
 * a field, the type is selected or built with {@link FieldBuilder#type()}.
 * <br>
 * Fields have default values that must be specified to complete the field.
 * {@link FieldDefault#noDefault()} is available for all field types, and
 * a specific method is available for each type to use a default, for example
 * {@link IntDefault#intDefault(int)}
 * <br>
 * There are field shortcut methods on {@link FieldAssembler} for primitive types.
 * These shortcuts create required, optional, and nullable fields, but do not
 * support field aliases, doc, or custom properties.
 *
 * <h2>Unions</h2>
 * Union types are built via {@link TypeBuilder#unionOf()} or
 * {@link FieldTypeBuilder#unionOf()} in the context of type selection.
 * This chains together multiple types, in union order.  For example:
 * <pre>
 * .unionOf()
 *   .fixed("IPv4").size(4).and()
 *   .fixed("IPv6").size(16).and()
 *   .nullType().endUnion()
 * </pre>
 * is equivalent to the Avro schema JSON:
 * <pre>
 * [
 *   {"type":"fixed", "name":"IPv4", "size":4},
 *   {"type":"fixed", "name":"IPv6", "size":16},
 *   "null"
 * ]
 * </pre>
 * In a field context, the first type of a union defines what default type
 * is allowed.
 * <br>
 * Unions have two shortcuts for common cases.  nullable()
 * creates a union of a type and null.  In a field type context, optional()
 * is available and creates a union of null and a type, with a null default.
 * The below two are equivalent:
 * <pre>
 *   .unionOf().intType().and().nullType().endUnion()
 *   .nullable().intType()
 * </pre>
 * The below two field declarations are equivalent:
 * <pre>
 *   .name("f").type().unionOf().nullType().and().longType().endUnion().nullDefault()
 *   .name("f").type().optional().longType()
 * </pre>
 *
 * <h2>Explicit Types and Types by Name</h2>
 * Types can also be specified explicitly by passing in a Schema, or by name:
 * <pre>
 *   .type(Schema.create(Schema.Type.INT)) // explicitly specified
 *   .type("MD5")                       // reference by full name or short name
 *   .type("MD5", "org.apache.avro.test")  // reference by name and namespace
 * </pre>
 * When a type is specified by name, and the namespace is absent or null, the
 * namespace is inherited from the enclosing context.  A namespace will
 * propagate as a default to child fields, nested types, or later defined types
 * in a union.  To specify a name that has no namespace and ignore the inherited
 * namespace, set the namespace to "".
 * <br>
 * {@link SchemaBuilder#builder(String)} returns a type builder with a default
 * namespace.  {@link SchemaBuilder#builder()} returns a type builder with no
 * default namespace.
 */
public class SchemaBuilder {

  private SchemaBuilder() {
  }

  /**
   * @return A builder for Avro Schemas
   */
  public static TypeBuilder<Schema> builder() {
    return new TypeBuilder<>(new SchemaCompletion(), new NameContext());
  }

  /**
   * Create a builder for Avro schemas with a default namespace. Types created
   * without namespaces will inherit the namespace provided.
   *
   * @param namespace The namespace of the Schema
   * @return A new TypeBuilder
   */
  public static TypeBuilder<Schema> builder(String namespace) {
    return new TypeBuilder<>(new SchemaCompletion(),
        new NameContext().namespace(namespace));
  }

  /**
   * Create a builder for an Avro record with the specified name.
   * This is equivalent to:
   * <pre>
   *   builder().record(name);
   * </pre>
   * @param name the record name
   * @return A new RecordBuilder
   */
  public static RecordBuilder<Schema> record(String name) {
    return builder().record(name);
  }

  /**
   * Create a builder for an Avro enum with the specified name and symbols (values).
   * This is equivalent to:
   * <pre>
   *   builder().enumeration(name);
   * </pre>
   * @param name the enum name
   * @return A new EnumBuilder
   */
  public static EnumBuilder<Schema> enumeration(String name) {
    return builder().enumeration(name);
  }

  /**
   * Create a builder for an Avro fixed type with the specified name and size.
   * This is equivalent to:
   * <pre>
   *   builder().fixed(name);
   * </pre>
   * @param name the fixed name
   * @return A new FixedBuilder
   */
  public static FixedBuilder<Schema> fixed(String name) {
    return builder().fixed(name);
  }

  /**
   * Create a builder for an Avro array
   * This is equivalent to:
   * <pre>
   *   builder().array();
   * </pre>
   * @return A new ArrayBuilder
   */
  public static ArrayBuilder<Schema> array() {
    return builder().array();
  }

  /**
   * Create a builder for an Avro map
   * This is equivalent to:
   * <pre>
   *   builder().map();
   * </pre>
   * @return A new MapBuilder
   */
  public static MapBuilder<Schema> map() {
    return builder().map();
  }

  /**
   * Create a builder for an Avro union
   * This is equivalent to:
   * <pre>
   *   builder().unionOf();
   * </pre>
   * @return A new BaseTypeBuilder
   */
  public static BaseTypeBuilder<UnionAccumulator<Schema>> unionOf() {
    return builder().unionOf();
  }

  /**
   * Create a builder for a union of a type and null.
   * This is a shortcut for:
   * <pre>
   *   builder().nullable();
   * </pre>
   * and the following two lines are equivalent:
   * <pre>
   *   nullable().intType();
   * </pre>
   * <pre>
   *   unionOf().intType().and().nullType().endUnion();
   * </pre>
   * @return A new BaseTypeBuilder
   */
  public static BaseTypeBuilder<Schema> nullable() {
    return builder().nullable();
  }


  /**
   * An abstract builder for all Avro types.  All Avro types
   * can have arbitrary string key-value properties.
   *
   */
  public static abstract class PropBuilder<S extends PropBuilder<S>> {
    private Map<String, JsonNode> props = null;
    protected PropBuilder() {
    }

    /**
     * Set name-value pair properties for this type or field.
     *
     * @param name The name of the property
     * @param val the value of the property
     * @return the contructed property
     */
    public final S prop(String name, String val) {
      return prop(name, TextNode.valueOf(val));
    }

    /**
     * Set name-value pair properties for this type or field.
     *
     * @param name The name of the property
     * @param value the value of the property
     * @return the contructed property
     */
    public final S prop(String name, Object value) {
      return prop(name, JacksonUtils.toJsonNode(value));
    }

    // for internal use by the Parser
    final S prop(String name, JsonNode val) {
      if(!hasProps()) {
        props = new HashMap<>();
      }
      props.put(name, val);
      return self();
    }

    private boolean hasProps() {
      return (props != null);
    }

    final <T extends JsonProperties> T addPropsTo(T jsonable) {
      if (hasProps()) {
        for(Map.Entry<String, JsonNode> prop : props.entrySet()) {
          jsonable.addProp(prop.getKey(), prop.getValue());
        }
      }
      return jsonable;
    }

    /**
     * A self-type for chaining builder subclasses. Concrete subclasses must return 'this'
     *
     * @return itself
     */
    protected abstract S self();
  }

  /**
   * An abstract type that provides builder methods for configuring the name,
   * doc, and aliases of all Avro types that have names (fields, Fixed, Record,
   * and Enum).
   * <br>
   * All Avro named types and fields have 'doc', 'aliases', and 'name'
   * components. 'name' is required, and provided to this builder. 'doc' and
   * 'aliases' are optional.
   */
  public static abstract class NamedBuilder<S extends NamedBuilder<S>> extends
      PropBuilder<S> {
    private final String name;
    private final NameContext names;
    private String doc;
    private String[] aliases;

    protected NamedBuilder(NameContext names, String name) {
      checkRequired(name, "Type must have a name");
      this.names = names;
      this.name = name;
    }

    /**
     * Configure this type's optional documentation string
     *
     * @param doc The docstring
     * @return {@link S}
     */
    public final S doc(String doc) {
      this.doc = doc;
      return self();
    }

    /**
     * Configure this type's optional name aliases
     *
     * @param aliases Set one or more aliasses
     * @return {@link S}
     */
    public final S aliases(String... aliases) {
      this.aliases = aliases;
      return self();
    }

    final String doc() {
      return doc;
    }

    final String name() {
      return name;
    }

    final NameContext names() {
      return names;
    }

    final Schema addAliasesTo(Schema schema) {
      if (null != aliases) {
        for (String alias : aliases) {
          schema.addAlias(alias);
        }
      }
      return schema;
    }

    final Field addAliasesTo(Field field) {
      if (null != aliases) {
        for (String alias : aliases) {
          field.addAlias(alias);
        }
      }
      return field;
    }
  }

  /**
   * An abstract type that provides builder methods for configuring the
   * namespace for all Avro types that have namespaces (Fixed, Record, and
   * Enum).
   */
  public static abstract class NamespacedBuilder<R, S extends NamespacedBuilder<R, S>>
      extends NamedBuilder<S> {
    private final Completion<R> context;
    private String namespace;

    protected NamespacedBuilder(Completion<R> context, NameContext names,
        String name) {
      super(names, name);
      this.context = context;
    }

    /**
     * Set the namespace of this type. To clear the namespace, set empty string.
     * <br>
     * When the namespace is null or unset, the namespace of the type defaults
     * to the namespace of the enclosing context.
     *
     * @param namespace The namespace of the schema
     * @return {@link S}
     */
    public final S namespace(String namespace) {
      this.namespace = namespace;
      return self();
    }

    final String space() {
      if (null == namespace) {
        return names().namespace;
      }
      return namespace;
    }

    final Schema completeSchema(Schema schema) {
      addPropsTo(schema);
      addAliasesTo(schema);
      names().put(schema);
      return schema;
    }

    final Completion<R> context() {
      return context;
    }
  }

  /**
   * An abstraction for sharing code amongst all primitive type builders.
   */
  private static abstract class PrimitiveBuilder<R, P extends PrimitiveBuilder<R, P>>
      extends PropBuilder<P> {
    private final Completion<R> context;
    private final Schema immutable;

    protected PrimitiveBuilder(Completion<R> context, NameContext names,
        Schema.Type type) {
      this.context = context;
      this.immutable = names.getFullname(type.getName());
    }

    private R end() {
      Schema schema = immutable;
      if (super.hasProps()) {
        schema = Schema.create(immutable.getType());
        addPropsTo(schema);
      }
      return context.complete(schema);
    }
  }

  /**
   * Builds an Avro boolean type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endBoolean()}
   */
  public static final class BooleanBuilder<R> extends
      PrimitiveBuilder<R, BooleanBuilder<R>> {
    private BooleanBuilder(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.BOOLEAN);
    }

    private static <R> BooleanBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new BooleanBuilder<>(context, names);
    }

    @Override
    protected BooleanBuilder<R> self() {
      return this;
    }

    /**
     * complete building this type, return control to context
     * @return {@link R}
     */
    public R endBoolean() {
      return super.end();
    }
  }

  /**
   * Builds an Avro int type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endInt()}
   */
  public static final class IntBuilder<R> extends
      PrimitiveBuilder<R, IntBuilder<R>> {
    private IntBuilder(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.INT);
    }

    private static <R> IntBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new IntBuilder<>(context, names);
    }

    @Override
    protected IntBuilder<R> self() {
      return this;
    }

    /**
     * Complete building this type, return control to context
     * @return {@link R}
     */
    public R endInt() {
      return super.end();
    }
  }

  /**
   * Builds an Avro long type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endLong()}
   */
  public static final class LongBuilder<R> extends
      PrimitiveBuilder<R, LongBuilder<R>> {
    private LongBuilder(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.LONG);
    }

    private static <R> LongBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new LongBuilder<>(context, names);
    }

    @Override
    protected LongBuilder<R> self() {
      return this;
    }

    /**
     * Complete building this type, return control to context
     * @return {@link R}
     */
    public R endLong() {
      return super.end();
    }
  }

  /**
   * Builds an Avro float type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endFloat()}
   */
  public static final class FloatBuilder<R> extends
      PrimitiveBuilder<R, FloatBuilder<R>> {
    private FloatBuilder(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.FLOAT);
    }

    private static <R> FloatBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new FloatBuilder<>(context, names);
    }

    @Override
    protected FloatBuilder<R> self() {
      return this;
    }

    /**
     * complete building this type, return control to context
     * @return {@link R}
     */
    public R endFloat() {
      return super.end();
    }
  }

  /**
   * Builds an Avro double type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endDouble()}
   */
  public static final class DoubleBuilder<R> extends
      PrimitiveBuilder<R, DoubleBuilder<R>> {
    private DoubleBuilder(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.DOUBLE);
    }

    private static <R> DoubleBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new DoubleBuilder<>(context, names);
    }

    @Override
    protected DoubleBuilder<R> self() {
      return this;
    }

    /**
     * Complete building this type, return control to context
     * @return {@link R}
     */
    public R endDouble() {
      return super.end();
    }
  }

  /**
   * Builds an Avro string type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endString()}
   */
  public static final class StringBldr<R> extends
      PrimitiveBuilder<R, StringBldr<R>> {
    private StringBldr(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.STRING);
    }

    private static <R> StringBldr<R> create(Completion<R> context,
        NameContext names) {
      return new StringBldr<>(context, names);
    }

    @Override
    protected StringBldr<R> self() {
      return this;
    }

    /**
     * complete building this type, return control to context
     * @return {@link R}
     */
    public R endString() {
      return super.end();
    }
  }

  /**
   * Builds an Avro bytes type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endBytes()}
   */
  public static final class BytesBuilder<R> extends
      PrimitiveBuilder<R, BytesBuilder<R>> {
    private BytesBuilder(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.BYTES);
    }

    private static <R> BytesBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new BytesBuilder<>(context, names);
    }

    @Override
    protected BytesBuilder<R> self() {
      return this;
    }

    /**
     * Complete building this type, return control to context
     * @return {@link R}
     */
    public R endBytes() {
      return super.end();
    }
  }

  /**
   * Builds an Avro null type with optional properties. Set properties with
   * {@link #prop(String, String)}, and finalize with {@link #endNull()}
   */
  public static final class NullBuilder<R> extends
      PrimitiveBuilder<R, NullBuilder<R>> {
    private NullBuilder(Completion<R> context, NameContext names) {
      super(context, names, Schema.Type.NULL);
    }

    private static <R> NullBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new NullBuilder<>(context, names);
    }

    @Override
    protected NullBuilder<R> self() {
      return this;
    }

    /**
     * Complete building this type, return control to context
     * @return {@link R}
     */
    public R endNull() {
      return super.end();
    }
  }

  /**
   * Builds an Avro Fixed type with optional properties, namespace, doc, and
   * aliases.
   * <br>
   * Set properties with {@link #prop(String, String)}, namespace with
   * {@link #namespace(String)}, doc with {@link #doc(String)}, and aliases with
   * {@link #aliases(String[])}.
   * <br>
   * The Fixed schema is finalized when its required size is set via
   * {@link #size(int)}.
   */
  public static final class FixedBuilder<R> extends
      NamespacedBuilder<R, FixedBuilder<R>> {
    private FixedBuilder(Completion<R> context, NameContext names, String name) {
      super(context, names, name);
    }

    private static <R> FixedBuilder<R> create(Completion<R> context,
        NameContext names, String name) {
      return new FixedBuilder<>(context, names, name);
    }

    @Override
    protected FixedBuilder<R> self() {
      return this;
    }

    /**
     * Complete building this type, return control to context
     *
     * @param size The size of the {@link FixedBuilder}
     * @return {@link R}
     */
    public R size(int size) {
      Schema schema = Schema.createFixed(name(), super.doc(), space(), size);
      completeSchema(schema);
      return context().complete(schema);
    }
  }

  /**
   * Builds an Avro Enum type with optional properties, namespace, doc, and
   * aliases.
   * <br>
   * Set properties with {@link #prop(String, String)}, namespace with
   * {@link #namespace(String)}, doc with {@link #doc(String)}, and aliases with
   * {@link #aliases(String[])}.
   * <br>
   * The Enum schema is finalized when its required symbols are set via
   * {@link #symbols(String[])}.
   */
  public static final class EnumBuilder<R> extends
      NamespacedBuilder<R, EnumBuilder<R>> {
    private EnumBuilder(Completion<R> context, NameContext names, String name) {
      super(context, names, name);
    }
    private String enumDefault = null;

    private static <R> EnumBuilder<R> create(Completion<R> context,
        NameContext names, String name) {
      return new EnumBuilder<>(context, names, name);
    }

    @Override
    protected EnumBuilder<R> self() {
      return this;
    }

    /**
     * Configure this enum type's symbols, and end its configuration. Populates the default if it was set.
     *
     * @param symbols One or more symbols to construct the enum
     * @return {@link R}
     */
    public R symbols(String... symbols) {
      Schema schema = Schema.createEnum(name(), doc(), space(),
          Arrays.asList(symbols), this.enumDefault);
      completeSchema(schema);
      return context().complete(schema);
    }

    /**
     * Set the default value of the enum.
     * @param enumDefault The default symbol
     * @return {@link R}
     */
    public EnumBuilder<R> defaultSymbol(String enumDefault) {
      this.enumDefault = enumDefault;
      return self();
    }
  }

  /**
   * Builds an Avro Map type with optional properties.
   * <br>
   * Set properties with {@link #prop(String, String)}.
   * <br>
   * The Map schema's properties are finalized when {@link #values()} or
   * {@link #values(Schema)} is called.
   */
  public static final class MapBuilder<R> extends PropBuilder<MapBuilder<R>> {
    private final Completion<R> context;
    private final NameContext names;

    private MapBuilder(Completion<R> context, NameContext names) {
      this.context = context;
      this.names = names;
    }

    private static <R> MapBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new MapBuilder<>(context, names);
    }

    @Override
    protected MapBuilder<R> self() {
      return this;
    }

    /**
     * Return a type builder for configuring the map's nested values schema.
     * This builder will return control to the map's enclosing context when
     * complete.
     *
     * @return {@link TypeBuilder}
     */
    public TypeBuilder<R> values() {
      return new TypeBuilder<>(new MapCompletion<>(this, context), names);
    }

    /**
     * Complete configuration of this map, setting the schema of the map values
     * to the schema provided. Returns control to the enclosing context.
     * @param valueSchema The schema to be set
     * @return {@link R}
     */
    public R values(Schema valueSchema) {
      return new MapCompletion<>(this, context).complete(valueSchema);
    }
  }

  /**
   * Builds an Avro Array type with optional properties.
   * <br>
   * Set properties with {@link #prop(String, String)}.
   * <br>
   * The Array schema's properties are finalized when {@link #items()} or
   * {@link #items(Schema)} is called.
   */
  public static final class ArrayBuilder<R> extends
      PropBuilder<ArrayBuilder<R>> {
    private final Completion<R> context;
    private final NameContext names;

    public ArrayBuilder(Completion<R> context, NameContext names) {
      this.context = context;
      this.names = names;
    }

    private static <R> ArrayBuilder<R> create(Completion<R> context,
        NameContext names) {
      return new ArrayBuilder<>(context, names);
    }

    @Override
    protected ArrayBuilder<R> self() {
      return this;
    }

    /**
     * Return a type builder for configuring the array's nested items schema.
     * This builder will return control to the array's enclosing context when
     * complete.
     *
     * @return The nested items of the schema
     */
    public TypeBuilder<R> items() {
      return new TypeBuilder<>(new ArrayCompletion<>(this, context), names);
    }

    /**
     * Complete configuration of this array, setting the schema of the array
     * items to the schema provided. Returns control to the enclosing context.
     *
     * @param itemsSchema Schema to complete the configuration
     * @return The enclosing context control
     */
    public R items(Schema itemsSchema) {
      return new ArrayCompletion<>(this, context).complete(itemsSchema);
    }
  }

  /**
   * internal class for passing the naming context around. This allows for the
   * following:
   * <ul>
   * <li>Cache and re-use primitive schemas when they do not set properties.</li>
   * <li>Provide a default namespace for nested contexts (as the JSON Schema spec does).</li>
   * <li>Allow previously defined named types or primitive types to be referenced by name.</li>
   * </ul>
   */
  private static class NameContext {
    private static final Set<String> PRIMITIVES = new HashSet<>();
    static {
      PRIMITIVES.add("null");
      PRIMITIVES.add("boolean");
      PRIMITIVES.add("int");
      PRIMITIVES.add("long");
      PRIMITIVES.add("float");
      PRIMITIVES.add("double");
      PRIMITIVES.add("bytes");
      PRIMITIVES.add("string");
    }
    private final HashMap<String, Schema> schemas;
    private final String namespace;

    private NameContext() {
      this.schemas = new HashMap<>();
      this.namespace = null;
      schemas.put("null", Schema.create(Schema.Type.NULL));
      schemas.put("boolean", Schema.create(Schema.Type.BOOLEAN));
      schemas.put("int", Schema.create(Schema.Type.INT));
      schemas.put("long", Schema.create(Schema.Type.LONG));
      schemas.put("float", Schema.create(Schema.Type.FLOAT));
      schemas.put("double", Schema.create(Schema.Type.DOUBLE));
      schemas.put("bytes", Schema.create(Schema.Type.BYTES));
      schemas.put("string", Schema.create(Schema.Type.STRING));
    }

    private NameContext(HashMap<String, Schema> schemas, String namespace) {
      this.schemas = schemas;
      this.namespace = "".equals(namespace) ? null : namespace;
    }

    private NameContext namespace(String namespace) {
      return new NameContext(schemas, namespace);
    }

    private Schema get(String name, String namespace) {
      return getFullname(resolveName(name, namespace));
    }

    private Schema getFullname(String fullName) {
      Schema schema = schemas.get(fullName);
      if(schema == null) {
        throw new SchemaParseException("Undefined name: " + fullName);
      }
      return schema;
    }

    private void put(Schema schema) {
      String fullName = schema.getFullName();
      if(schemas.containsKey(fullName)){
        throw new SchemaParseException("Can't redefine: " + fullName);
     }
     schemas.put(fullName, schema);
    }

    private String resolveName(String name, String space) {
      if (PRIMITIVES.contains(name) && space == null) {
        return name;
      }
      int lastDot = name.lastIndexOf('.');
      if (lastDot < 0) { // short name
        if (space == null) {
          space = namespace;
        }
        if (space != null && !"".equals(space)) {
          return space + "." + name;
        }
      }
      return name;
    }
  }

  /**
   * A common API for building types within a context. BaseTypeBuilder can build
   * all types other than Unions. {@link TypeBuilder} can additionally build
   * Unions.
   * <br>
   * The builder has two contexts:
   * <ul>
   * <li>A naming context provides a default namespace and allows for previously
   * defined named types to be referenced from {@link #type(String)}</li>
   * <li>A completion context representing the scope that the builder was
   * created in. A builder created in a nested context (for example,
   * {@link MapBuilder#values()} will have a completion context assigned by the
   * {@link MapBuilder}</li>
   * </ul>
   */
  public static class BaseTypeBuilder<R> {
    private final Completion<R> context;
    private final NameContext names;

    private BaseTypeBuilder(Completion<R> context, NameContext names) {
      this.context = context;
      this.names = names;
    }

    /**
     * Use the schema provided as the type.
     *
     * @param schema The provided schema
     * @return The completed record
     */
    public final R type(Schema schema) {
      return context.complete(schema);
    }

    /**
     * Look up the type by name. This type must be previously defined in the
     * context of this builder.
     * <br>
     * The name may be fully qualified or a short name. If it is a short name,
     * the default namespace of the current context will additionally be
     * searched.
     *
     * @param name The lookup name
     * @return The completed record
     */
    public final R type(String name) {
      return type(name, null);
    }

    /**
     * Look up the type by name and namespace. This type must be previously
     * defined in the context of this builder.
     * <br>
     * The name may be fully qualified or a short name. If it is a fully
     * qualified name, the namespace provided is ignored. If it is a short name,
     * the namespace provided is used if not null, else the default namespace of
     * the current context will be used.
     *
     * @param name The lookup name
     * @param namespace The namespace to search in
     * @return The completed record
     */
    public final R type(String name, String namespace) {
      return type(names.get(name, namespace));
    }

    /**
     * A plain boolean type without custom properties. This is equivalent to:
     * <pre>
     * booleanBuilder().endBoolean();
     * </pre>
     *
     * @return Record
     */
    public final R booleanType() {
      return booleanBuilder().endBoolean();
    }

    /**
     * Build a boolean type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #booleanType()}.
     *
     * @return {@link BooleanBuilder}
     */
    public final BooleanBuilder<R> booleanBuilder() {
      return BooleanBuilder.create(context, names);
    }

    /**
     * A plain int type without custom properties. This is equivalent to:
     * <pre>
     * intBuilder().endInt();
     * </pre>
     *
     * @return Record
     */
    public final R intType() {
      return intBuilder().endInt();
    }

    /**
     * Build an int type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #intType()}.
     *
     * @return {@link IntBuilder}
     */
    public final IntBuilder<R> intBuilder() {
      return IntBuilder.create(context, names);
    }

    /**
     * A plain long type without custom properties. This is equivalent to:
     * <pre>
     * longBuilder().endLong();
     * </pre>
     *
     * @return Record
     */
    public final R longType() {
      return longBuilder().endLong();
    }

    /**
     * Build a long type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #longType()}.
     *
     * @return {@link LongBuilder}
     */
    public final LongBuilder<R> longBuilder() {
      return LongBuilder.create(context, names);
    }

    /**
     * A plain float type without custom properties. This is equivalent to:
     * <pre>
     * floatBuilder().endFloat();
     * </pre>
     *
     * @return {@link R}
     */
    public final R floatType() {
      return floatBuilder().endFloat();
    }

    /**
     * Build a float type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #floatType()}.
     *
     * @return {@link FloatBuilder}
     */
    public final FloatBuilder<R> floatBuilder() {
      return FloatBuilder.create(context, names);
    }

    /**
     * A plain double type without custom properties. This is equivalent to:
     * <pre>
     * doubleBuilder().endDouble();
     * </pre>
     *
     * @return Record
     */
    public final R doubleType() {
      return doubleBuilder().endDouble();
    }

    /**
     * Build a double type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #doubleType()}.
     *
     * @return {@link DoubleBuilder}
     */
    public final DoubleBuilder<R> doubleBuilder() {
      return DoubleBuilder.create(context, names);
    }

    /**
     * A plain string type without custom properties. This is equivalent to:
     * <pre>
     * stringBuilder().endString();
     * </pre>
     *
     * @return Record
     */
    public final R stringType() {
      return stringBuilder().endString();
    }

    /**
     * Build a string type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #stringType()}.
     *
     * @return {@link StringBldr}
     */
    public final StringBldr<R> stringBuilder() {
      return StringBldr.create(context, names);
    }

    /**
     * A plain bytes type without custom properties. This is equivalent to:
     * <pre>
     * bytesBuilder().endBytes();
     * </pre>
     *
     * @return Record
     */
    public final R bytesType() {
      return bytesBuilder().endBytes();
    }

    /**
     * Build a bytes type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #bytesType()}.
     *
     * @return {@link BytesBuilder}
     */
    public final BytesBuilder<R> bytesBuilder() {
      return BytesBuilder.create(context, names);
    }

    /**
     * A plain null type without custom properties. This is equivalent to:
     * <pre>
     * nullBuilder().endNull();
     * </pre>
     *
     * @return Record
     */
    public final R nullType() {
      return nullBuilder().endNull();
    }

    /**
     * Build a null type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #nullType()}.
     *
     * @return {@link NullBuilder}
     */
    public final NullBuilder<R> nullBuilder() {
      return NullBuilder.create(context, names);
    }

    /** Build an Avro map type  Example usage:
     * <pre>
     * map().values().intType()
     * </pre>
     * Equivalent to Avro JSON Schema:
     * <pre>
     * {"type":"map", "values":"int"}
     * </pre>
     *
     * @return {@link MapBuilder}
     */
    public final MapBuilder<R> map() {
      return MapBuilder.create(context, names);
    }

    /**
     * Build an Avro array type  Example usage:
     * <pre>
     * array().items().longType()
     * </pre>
     * Equivalent to Avro JSON Schema:
     * <pre>
     * {"type":"array", "values":"long"}
     * </pre>
     *
     * @return {@link ArrayBuilder}
     */
    public final ArrayBuilder<R> array() {
      return ArrayBuilder.create(context, names);
    }

    /**
     * Build an Avro fixed type. Example usage:
     * <pre>
     * fixed("com.foo.IPv4").size(4)
     * </pre>
     * Equivalent to Avro JSON Schema:
     * <pre>
     * {"type":"fixed", "name":"com.foo.IPv4", "size":4}
     * </pre>
     *
     * @param name The name of the field
     * @return {@link FixedBuilder}
     */
    public final FixedBuilder<R> fixed(String name) {
      return FixedBuilder.create(context, names, name);
    }

    /** Build an Avro enum type. Example usage:
     * <pre>
     * enumeration("Suits").namespace("org.cards").doc("card suit names").defaultSymbol("HEART")
     *   .symbols("HEART", "SPADE", "DIAMOND", "CLUB")
     * </pre>
     * Equivalent to Avro JSON Schema:
     * <pre>
     * {"type":"enum", "name":"Suits", "namespace":"org.cards",
     *  "doc":"card suit names", "symbols":[
     *    "HEART", "SPADE", "DIAMOND", "CLUB"], "default":"HEART"}
     * </pre>
     *
     * @param name The name of the field
     * @return {@link EnumBuilder}
     */
    public final EnumBuilder<R> enumeration(String name) {
      return EnumBuilder.create(context, names, name);
    }

    /** Build an Avro record type. Example usage:
     * <pre>
     * record("com.foo.Foo").fields()
     *   .name("field1").typeInt().intDefault(1)
     *   .name("field2").typeString().noDefault()
     *   .name("field3").optional().typeFixed("FooFixed").size(4)
     *   .endRecord()
     * </pre>
     * Equivalent to Avro JSON Schema:
     * <pre>
     * {"type":"record", "name":"com.foo.Foo", "fields": [
     *   {"name":"field1", "type":"int", "default":1},
     *   {"name":"field2", "type":"string"},
     *   {"name":"field3", "type":[
     *     null, {"type":"fixed", "name":"FooFixed", "size":4}
     *     ]}
     *   ]}
     * </pre>
     *
     * @param name The name of the field
     * @return {@link RecordBuilder}
     */
    public final RecordBuilder<R> record(String name) {
      return RecordBuilder.create(context, names, name);
    }

    /** Build an Avro union schema type. Example usage:
     * <pre>unionOf().stringType().and().bytesType().endUnion()</pre>
     *
     * @return {@link BaseTypeBuilder}
     */
    protected BaseTypeBuilder<UnionAccumulator<R>> unionOf() {
      return UnionBuilder.create(context, names);
    }

    /** A shortcut for building a union of a type and null.
     * <br>
     * For example, the code snippets below are equivalent:
     * <pre>nullable().booleanType()</pre>
     * <pre>unionOf().booleanType().and().nullType().endUnion()</pre>
     *
     * @return {@link BaseTypeBuilder}
     */
    protected BaseTypeBuilder<R> nullable() {
      return new BaseTypeBuilder<>(new NullableCompletion<>(context), names);
    }

  }

  /**
   * A Builder for creating any Avro schema type.
   */
  public static final class TypeBuilder<R> extends BaseTypeBuilder<R> {
    private TypeBuilder(Completion<R> context, NameContext names) {
      super(context, names);
    }

    /**
     * @return {@link BaseTypeBuilder}
     */
    @Override
    public BaseTypeBuilder<UnionAccumulator<R>> unionOf() {
      return super.unionOf();
    }

    /**
     * @return {@link BaseTypeBuilder}
     */
    @Override
    public BaseTypeBuilder<R> nullable() {
      return super.nullable();
    }
  }

  /**
   * A special builder for unions.  Unions cannot nest unions directly
   */
  private static final class UnionBuilder<R> extends
      BaseTypeBuilder<UnionAccumulator<R>> {
    private UnionBuilder(Completion<R> context, NameContext names) {
      this(context, names, new ArrayList<>());
    }

    private static <R> UnionBuilder<R> create(Completion<R> context, NameContext names) {
      return new UnionBuilder<>(context, names);
    }

    private UnionBuilder(Completion<R> context, NameContext names, List<Schema> schemas) {
      super(new UnionCompletion<>(context, names, schemas), names);
    }
  }

  /**
   * A special Builder for Record fields. The API is very similar to
   * {@link BaseTypeBuilder}. However, fields have their own names, properties,
   * and default values.
   * <br>
   * The methods on this class create builder instances that return their
   * control to the {@link FieldAssembler} of the enclosing record context after
   * configuring a default for the field.
   * <br>
   * For example, an int field with default value 1:
   * <pre>
   * intSimple().withDefault(1);
   * </pre>
   * or an array with items that are optional int types:
   * <pre>
   * array().items().optional().intType();
   * </pre>
   */
  public static class BaseFieldTypeBuilder<R> {
    protected final FieldBuilder<R> bldr;
    protected final NameContext names;
    private final CompletionWrapper wrapper;

    protected BaseFieldTypeBuilder(FieldBuilder<R> bldr, CompletionWrapper wrapper) {
      this.bldr = bldr;
      this.names = bldr.names();
      this.wrapper = wrapper;
    }

    /**
     * A plain boolean type without custom properties. This is equivalent to:
     * <pre>
     * booleanBuilder().endBoolean();
     * </pre>
     *
     * @return {@link BooleanDefault}
     */
    public final BooleanDefault<R> booleanType() {
      return booleanBuilder().endBoolean();
    }

    /**
     * Build a boolean type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #booleanType()}.
     *
     * @return {@link BooleanBuilder}
     */
    public final BooleanBuilder<BooleanDefault<R>> booleanBuilder() {
      return BooleanBuilder.create(wrap(new BooleanDefault<>(bldr)), names);
    }

    /**
     * A plain int type without custom properties. This is equivalent to:
     * <pre>
     * intBuilder().endInt();
     * </pre>
     *
     * @return {@link IntDefault}
     */
    public final IntDefault<R> intType() {
      return intBuilder().endInt();
    }

    /**
     * Build an int type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #intType()}.
     *
     * @return {@link IntBuilder}
     */
    public final IntBuilder<IntDefault<R>> intBuilder() {
      return IntBuilder.create(wrap(new IntDefault<>(bldr)), names);
    }

    /**
     * A plain long type without custom properties. This is equivalent to:
     * <pre>
     * longBuilder().endLong();
     * </pre>
     *
     * @return {@link LongDefault}
     */
    public final LongDefault<R> longType() {
      return longBuilder().endLong();
    }

    /**
     * Build a long type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #longType()}.
     *
     * @return {@link LongBuilder}
     */
    public final LongBuilder<LongDefault<R>> longBuilder() {
      return LongBuilder.create(wrap(new LongDefault<>(bldr)), names);
    }

    /**
     * A plain float type without custom properties. This is equivalent to:
     * <pre>
     * floatBuilder().endFloat();
     * </pre>
     *
     * @return {@link FloatDefault}
     */
    public final FloatDefault<R> floatType() {
      return floatBuilder().endFloat();
    }

    /**
     * Build a float type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #floatType()}.
     *
     * @return {@link FloatBuilder}
     */
    public final FloatBuilder<FloatDefault<R>> floatBuilder() {
      return FloatBuilder.create(wrap(new FloatDefault<>(bldr)), names);
    }

    /**
     * A plain double type without custom properties. This is equivalent to:
     * <pre>
     * doubleBuilder().endDouble();
     * </pre>
     *
     * @return {@link DoubleDefault}
     */
    public final DoubleDefault<R> doubleType() {
      return doubleBuilder().endDouble();
    }

    /**
     * Build a double type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #doubleType()}.
     *
     * @return {@link DoubleBuilder}
     */
    public final DoubleBuilder<DoubleDefault<R>> doubleBuilder() {
      return DoubleBuilder.create(wrap(new DoubleDefault<>(bldr)), names);
    }

    /**
     * A plain string type without custom properties. This is equivalent to:
     * <pre>
     * stringBuilder().endString();
     * </pre>
     *
     * @return {@link StringDefault}
     */
    public final StringDefault<R> stringType() {
      return stringBuilder().endString();
    }

    /**
     * Build a string type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #stringType()}.
     *
     * @return {@link StringBldr}
     */
    public final StringBldr<StringDefault<R>> stringBuilder() {
      return StringBldr.create(wrap(new StringDefault<>(bldr)), names);
    }

    /**
     * A plain bytes type without custom properties. This is equivalent to:
     * <pre>
     * bytesBuilder().endBytes();
     * </pre>
     *
     * @return {@link BytesDefault}
     */
    public final BytesDefault<R> bytesType() {
      return bytesBuilder().endBytes();
    }

    /**
     * Build a bytes type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #bytesType()}.
     *
     * @return {@link BytesBuilder}
     */
    public final BytesBuilder<BytesDefault<R>> bytesBuilder() {
      return BytesBuilder.create(wrap(new BytesDefault<>(bldr)), names);
    }

    /**
     * A plain null type without custom properties. This is equivalent to:
     * <pre>
     * nullBuilder().endNull();
     * </pre>
     *
     * @return {@link NullDefault}
     */
    public final NullDefault<R> nullType() {
      return nullBuilder().endNull();
    }

    /**
     * Build a null type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #nullType()}.
     *
     * @return {@link NullBuilder}
     */
    public final NullBuilder<NullDefault<R>> nullBuilder() {
      return NullBuilder.create(wrap(new NullDefault<>(bldr)), names);
    }

    /**
     * Build an Avro map type
     *
     * @return {@link MapBuilder}
     */
    public final MapBuilder<MapDefault<R>> map() {
      return MapBuilder.create(wrap(new MapDefault<>(bldr)), names);
    }

    /**
     * Build an Avro array type
     *
     * @return {@link ArrayBuilder}
     */
    public final ArrayBuilder<ArrayDefault<R>> array() {
      return ArrayBuilder.create(wrap(new ArrayDefault<>(bldr)), names);
    }

    /**
     * Build an Avro fixed type
     *
     * @param name The name of the type
     * @return {@link FixedBuilder}
     */
    public final FixedBuilder<FixedDefault<R>> fixed(String name) {
      return FixedBuilder.create(wrap(new FixedDefault<>(bldr)), names, name);
    }

    /**
     * Build an Avro enum type.
     *
     * @param name The name of the enumeration
     * @return {@link EnumBuilder}
     */
    public final EnumBuilder<EnumDefault<R>> enumeration(String name) {
      return EnumBuilder.create(wrap(new EnumDefault<>(bldr)), names, name);
    }

    /**
     * Build an Avro record type.
     *
     * @param name The name of the record
     * @return {@link RecordBuilder}
     */
    public final RecordBuilder<RecordDefault<R>> record(String name) {
      return RecordBuilder.create(wrap(new RecordDefault<>(bldr)), names, name);
    }

    private <C> Completion<C> wrap(
       Completion<C> completion) {
      if (wrapper != null) {
        return wrapper.wrap(completion);
      }
      return completion;
    }
  }

  /**
   * FieldTypeBuilder adds {@link #unionOf()}, {@link #nullable()}, and {@link #optional()}
   * to BaseFieldTypeBuilder.
   */
  public static final class FieldTypeBuilder<R> extends BaseFieldTypeBuilder<R> {
    private FieldTypeBuilder(FieldBuilder<R> bldr) {
      super(bldr, null);
    }

    /**
     * Build an Avro union schema type.
     *
     * @return UnionFieldTypeBuilder
     */
    public UnionFieldTypeBuilder<R> unionOf() {
      return new UnionFieldTypeBuilder<>(bldr);
    }

    /**
     * A shortcut for building a union of a type and null, with an optional default
     * value of the non-null type.
     * <br>
     * For example, the two code snippets below are equivalent:
     * <pre>nullable().booleanType().booleanDefault(true)</pre>
     * <pre>unionOf().booleanType().and().nullType().endUnion().booleanDefault(true)</pre>
     *
     * @return BaseFieldTypeBuilder
     */
    public BaseFieldTypeBuilder<R> nullable() {
      return new BaseFieldTypeBuilder<>(bldr, new NullableCompletionWrapper());
    }

    /**
     * A shortcut for building a union of null and a type, with a null default.
     * <br>
     * For example, the two code snippets below are equivalent:
     * <pre>optional().booleanType()</pre>
     * <pre>unionOf().nullType().and().booleanType().endUnion().nullDefault()</pre>
     *
     * @return BaseTypeBuilder
     */
    public BaseTypeBuilder<FieldAssembler<R>> optional() {
      return new BaseTypeBuilder<>(
          new OptionalCompletion<>(bldr), names);
    }
  }

  /** Builder for a union field.  The first type in the union corresponds
   * to the possible default value type.
   */
  public static final class UnionFieldTypeBuilder<R> {
    private final FieldBuilder<R> bldr;
    private final NameContext names;

    private UnionFieldTypeBuilder(FieldBuilder<R> bldr) {
      this.bldr = bldr;
      this.names = bldr.names();
    }

    /**
     * A plain boolean type without custom properties. This is equivalent to:
     * <pre>
     * booleanBuilder().endBoolean();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<BooleanDefault<R>> booleanType() {
      return booleanBuilder().endBoolean();
    }

    /**
     * Build a boolean type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #booleanType()}.
     *
     * @return BooleanBuilder
     */
    public BooleanBuilder<UnionAccumulator<BooleanDefault<R>>> booleanBuilder() {
      return BooleanBuilder.create(completion(new BooleanDefault<>(bldr)), names);
    }

    /**
     * A plain int type without custom properties. This is equivalent to:
     * <pre>
     * intBuilder().endInt();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<IntDefault<R>> intType() {
      return intBuilder().endInt();
    }

    /**
     * Build an int type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #intType()}.
     *
     * @return IntBuilder
     */
    public IntBuilder<UnionAccumulator<IntDefault<R>>> intBuilder() {
      return IntBuilder.create(completion(new IntDefault<>(bldr)), names);
    }

    /**
     * A plain long type without custom properties. This is equivalent to:
     * <pre>
     * longBuilder().endLong();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<LongDefault<R>> longType() {
      return longBuilder().endLong();
    }

    /**
     * Build a long type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #longType()}.
     *
     * @return LongBuilder
     */
    public LongBuilder<UnionAccumulator<LongDefault<R>>> longBuilder() {
      return LongBuilder.create(completion(new LongDefault<>(bldr)), names);
    }

    /**
     * A plain float type without custom properties. This is equivalent to:
     * <pre>
     * floatBuilder().endFloat();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<FloatDefault<R>> floatType() {
      return floatBuilder().endFloat();
    }

    /**
     * Build a float type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #floatType()}.
     *
     * @return FloatBuilder
     */
    public FloatBuilder<UnionAccumulator<FloatDefault<R>>> floatBuilder() {
      return FloatBuilder.create(completion(new FloatDefault<>(bldr)), names);
    }

    /**
     * A plain double type without custom properties. This is equivalent to:
     * <pre>
     * doubleBuilder().endDouble();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<DoubleDefault<R>> doubleType() {
      return doubleBuilder().endDouble();
    }

    /**
     * Build a double type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #doubleType()}.
     *
     * @return DoubleBuilder
     */
    public DoubleBuilder<UnionAccumulator<DoubleDefault<R>>> doubleBuilder() {
      return DoubleBuilder.create(completion(new DoubleDefault<>(bldr)), names);
    }

    /**
     * A plain string type without custom properties. This is equivalent to:
     * <pre>
     * stringBuilder().endString();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<StringDefault<R>> stringType() {
      return stringBuilder().endString();
    }

    /**
     * Build a string type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #stringType()}.
     *
     * @return StringBldr
     */
    public StringBldr<UnionAccumulator<StringDefault<R>>> stringBuilder() {
      return StringBldr.create(completion(new StringDefault<>(bldr)), names);
    }

    /**
     * A plain bytes type without custom properties. This is equivalent to:
     * <pre>
     * bytesBuilder().endBytes();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<BytesDefault<R>> bytesType() {
      return bytesBuilder().endBytes();
    }

    /**
     * Build a bytes type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #bytesType()}.
     *
     * @return BytesBuilder
     */
    public BytesBuilder<UnionAccumulator<BytesDefault<R>>> bytesBuilder() {
      return BytesBuilder.create(completion(new BytesDefault<>(bldr)), names);
    }

    /**
     * A plain null type without custom properties. This is equivalent to:
     * <pre>
     * nullBuilder().endNull();
     * </pre>
     *
     * @return UnionAccumulator
     */
    public UnionAccumulator<NullDefault<R>> nullType() {
      return nullBuilder().endNull();
    }

    /**
     * Build a null type that can set custom properties. If custom properties
     * are not needed it is simpler to use {@link #nullType()}.
     *
     * @return NullBuilder
     */
    public NullBuilder<UnionAccumulator<NullDefault<R>>> nullBuilder() {
      return NullBuilder.create(completion(new NullDefault<>(bldr)), names);
    }

    /**
     * Build an Avro map type
     *
     * @return MapBuilder
     */
    public MapBuilder<UnionAccumulator<MapDefault<R>>> map() {
      return MapBuilder.create(completion(new MapDefault<>(bldr)), names);
    }

    /**
     * Build an Avro array type
     *
     * @return ArrayBuilder
     */
    public ArrayBuilder<UnionAccumulator<ArrayDefault<R>>> array() {
      return ArrayBuilder.create(completion(new ArrayDefault<>(bldr)), names);
    }

    /**
     * Build an Avro fixed type.
     *
     * @param name The name of the type
     * @return {@link FixedBuilder}
     */
    public FixedBuilder<UnionAccumulator<FixedDefault<R>>> fixed(String name) {
      return FixedBuilder.create(completion(new FixedDefault<>(bldr)), names, name);
    }

    /**
     * Build an Avro enum type.
     *
     * @param name The name of the type
     * @return EnumBuilder
     */
    public EnumBuilder<UnionAccumulator<EnumDefault<R>>> enumeration(String name) {
      return EnumBuilder.create(completion(new EnumDefault<>(bldr)), names, name);
    }

    /**
     * Build an Avro record type.
     *
     * @param name The name of the type
     * @return RecordBuilder
     */
    public RecordBuilder<UnionAccumulator<RecordDefault<R>>> record(String name) {
      return RecordBuilder.create(completion(new RecordDefault<>(bldr)), names, name);
    }

    /**
     * @param context The completion context
     * @return A completed UnionCompletion
     */
    private <C> UnionCompletion<C> completion(Completion<C> context) {
      return new UnionCompletion<>(context, names, new ArrayList<>());
    }
  }

  public final static class RecordBuilder<R> extends
      NamespacedBuilder<R, RecordBuilder<R>> {
    private RecordBuilder(Completion<R> context, NameContext names, String name) {
      super(context, names, name);
    }

    private static <R> RecordBuilder<R> create(Completion<R> context,
        NameContext names, String name) {
      return new RecordBuilder<>(context, names, name);
    }

    @Override
    protected RecordBuilder<R> self() {
      return this;
    }

    /**
     * @return FieldAssembler
     */
    public FieldAssembler<R> fields() {
      Schema record = Schema.createRecord(name(), doc(), space(), false);
      // place the record in the name context, fields yet to be set.
      completeSchema(record);
      return new FieldAssembler<>(
          context(), names().namespace(record.getNamespace()), record);
    }
  }

  public final static class FieldAssembler<R> {
    private final List<Field> fields = new ArrayList<>();
    private final Completion<R> context;
    private final NameContext names;
    private final Schema record;

    private FieldAssembler(Completion<R> context, NameContext names, Schema record) {
      this.context = context;
      this.names = names;
      this.record = record;
    }

    /**
     * Add a field with the given name.
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldBuilder} for the given name.
     */
    public FieldBuilder<R> name(String fieldName) {
      return new FieldBuilder<>(this, names, fieldName);
    }

    /**
     * Shortcut for creating a boolean field with the given name and no default.
     * <br>This is equivalent to:
     * <pre>
     *   name(fieldName).type().booleanType().noDefault()
     * </pre>
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name.
     */
    public FieldAssembler<R> requiredBoolean(String fieldName) {
      return name(fieldName).type().booleanType().noDefault();
    }

    /**
     * Shortcut for creating an optional boolean field: a union of null and
     * boolean with null default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().optional().booleanType()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> optionalBoolean(String fieldName) {
      return name(fieldName).type().optional().booleanType();
    }

    /**
     * Shortcut for creating a nullable boolean field: a union of boolean and
     * null with an boolean default.
     * <br>
     * This is equivalent to:
     *
     * <pre>
     * name(fieldName).type().nullable().booleanType().booleanDefault(defaultVal)
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @param defaultVal The default value
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> nullableBoolean(String fieldName, boolean defaultVal) {
      return name(fieldName)
          .type().nullable().booleanType().booleanDefault(defaultVal);
    }

    /**
     * Shortcut for creating an int field with the given name and no default.
     * <br>This is equivalent to:
     * <pre>
     *   name(fieldName).type().intType().noDefault()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> requiredInt(String fieldName) {
      return name(fieldName).type().intType().noDefault();
    }

    /**
     * Shortcut for creating an optional int field: a union of null and int
     * with null default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().optional().intType()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> optionalInt(String fieldName) {
      return name(fieldName).type().optional().intType();
    }

    /**
     * Shortcut for creating a nullable int field: a union of int and null
     * with an int default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().nullable().intType().intDefault(defaultVal)
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @param defaultVal The default value
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> nullableInt(String fieldName, int defaultVal) {
      return name(fieldName).type().nullable().intType().intDefault(defaultVal);
    }

    /**
     * Shortcut for creating a long field with the given name and no default.
     * <br>This is equivalent to:
     * <pre>
     *   name(fieldName).type().longType().noDefault()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> requiredLong(String fieldName) {
      return name(fieldName).type().longType().noDefault();
    }

    /**
     * Shortcut for creating an optional long field: a union of null and long
     * with null default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().optional().longType()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> optionalLong(String fieldName) {
      return name(fieldName).type().optional().longType();
    }

    /**
     * Shortcut for creating a nullable long field: a union of long and null
     * with a long default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().nullable().longType().longDefault(defaultVal)
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @param defaultVal The default value
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> nullableLong(String fieldName, long defaultVal) {
      return name(fieldName).type().nullable().longType().longDefault(defaultVal);
    }

    /**
     * Shortcut for creating a float field with the given name and no default.
     * <br>This is equivalent to:
     * <pre>
     *   name(fieldName).type().floatType().noDefault()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> requiredFloat(String fieldName) {
      return name(fieldName).type().floatType().noDefault();
    }

    /**
     * Shortcut for creating an optional float field: a union of null and float
     * with null default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().optional().floatType()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> optionalFloat(String fieldName) {
      return name(fieldName).type().optional().floatType();
    }

    /**
     * Shortcut for creating a nullable float field: a union of float and null
     * with a float default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().nullable().floatType().floatDefault(defaultVal)
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @param defaultVal The default value
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> nullableFloat(String fieldName, float defaultVal) {
      return name(fieldName).type().nullable().floatType().floatDefault(defaultVal);
    }

    /**
     * Shortcut for creating a double field with the given name and no default.
     * <br>This is equivalent to:
     * <pre>
     *   name(fieldName).type().doubleType().noDefault()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> requiredDouble(String fieldName) {
      return name(fieldName).type().doubleType().noDefault();
    }

    /**
     * Shortcut for creating an optional double field: a union of null and double
     * with null default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().optional().doubleType()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> optionalDouble(String fieldName) {
      return name(fieldName).type().optional().doubleType();
    }

    /**
     * Shortcut for creating a nullable double field: a union of double and null
     * with a double default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().nullable().doubleType().doubleDefault(defaultVal)
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @param defaultVal The default value
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> nullableDouble(String fieldName, double defaultVal) {
      return name(fieldName).type().nullable().doubleType().doubleDefault(defaultVal);
    }

    /**
     * Shortcut for creating a string field with the given name and no default.
     * <br>This is equivalent to:
     * <pre>
     *   name(fieldName).type().stringType().noDefault()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> requiredString(String fieldName) {
      return name(fieldName).type().stringType().noDefault();
    }

    /**
     * Shortcut for creating an optional string field: a union of null and string
     * with null default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().optional().stringType()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> optionalString(String fieldName) {
      return name(fieldName).type().optional().stringType();
    }

    /**
     * Shortcut for creating a nullable string field: a union of string and null
     * with a string default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().nullable().stringType().stringDefault(defaultVal)
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @param defaultVal The default value
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> nullableString(String fieldName, String defaultVal) {
      return name(fieldName).type().nullable().stringType().stringDefault(defaultVal);
    }

    /**
     * Shortcut for creating a bytes field with the given name and no default.
     * <br>This is equivalent to:
     * <pre>
     *   name(fieldName).type().bytesType().noDefault()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> requiredBytes(String fieldName) {
      return name(fieldName).type().bytesType().noDefault();
    }

    /**
     * Shortcut for creating an optional bytes field: a union of null and bytes
     * with null default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().optional().bytesType()
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> optionalBytes(String fieldName) {
      return name(fieldName).type().optional().bytesType();
    }

    /**
     * Shortcut for creating a nullable bytes field: a union of bytes and null
     * with a bytes default.<br>
     * This is equivalent to:
     * <pre>
     *   name(fieldName).type().nullable().bytesType().bytesDefault(defaultVal)
     * </pre>
     *
     * @param fieldName The fieldname of the builder
     * @param defaultVal The default value
     * @return A {@link FieldAssembler} for the given name, if available
     */
    public FieldAssembler<R> nullableBytes(String fieldName, byte[] defaultVal) {
      return name(fieldName).type().nullable().bytesType().bytesDefault(defaultVal);
    }

    /**
     * End adding fields to this record, returning control
     * to the context that this record builder was created in.
     *
     * @return The closed record
     */
    public R endRecord() {
      record.setFields(fields);
      return context.complete(record);
    }

    private FieldAssembler<R> addField(Field field) {
      fields.add(field);
      return this;
    }

  }

  /**
   * Builds a Field in the context of a {@link FieldAssembler}.
   *
   * Usage is to first configure any of the optional parameters and then to call one
   * of the type methods to complete the field.  For example
   * <pre>
   *   .namespace("org.apache.example").orderDescending().type()
   * </pre>
   * Optional parameters for a field are namespace, doc, order, and aliases.
   */
  public final static class FieldBuilder<R> extends
      NamedBuilder<FieldBuilder<R>> {
    private final FieldAssembler<R> fields;
    private Schema.Field.Order order = Schema.Field.Order.ASCENDING;

    private FieldBuilder(FieldAssembler<R> fields, NameContext names, String name) {
      super(names, name);
      this.fields = fields;
    }

    /**
     * Set this field to have ascending order.  Ascending is the default
     * @return {@link FieldBuilder}
     */
    public FieldBuilder<R> orderAscending() {
      order = Schema.Field.Order.ASCENDING;
      return self();
    }

    /**
     * Set this field to have descending order.  Descending is the default
     * @return {@link FieldBuilder}
     */
    public FieldBuilder<R> orderDescending() {
      order = Schema.Field.Order.DESCENDING;
      return self();
    }

    /**
     * Set this field to ignore order.
     * @return {@link FieldBuilder}
     */
    public FieldBuilder<R> orderIgnore() {
      order = Schema.Field.Order.IGNORE;
      return self();
    }

    /**
     * Final step in configuring this field, finalizing name, namespace, alias,
     * and order.
     * @return A builder for the field's type and default value.
     */
    public FieldTypeBuilder<R> type() {
      return new FieldTypeBuilder<>(this);
    }

    /**
     * Final step in configuring this field, finalizing name, namespace, alias,
     * and order.  Sets the field's type to the provided schema, returns a
     * {@link GenericDefault}.
     *
     * @param type The type of the schema
     * @return {@link GenericDefault}
     */
    public GenericDefault<R> type(Schema type) {
      return new GenericDefault<>(this, type);
    }

    /**
     * Final step in configuring this field, finalizing name, namespace, alias,
     * and order. Sets the field's type to the schema by name reference.
     * <br>
     * The name must correspond with a named schema that has already been
     * created in the context of this builder. The name may be a fully qualified
     * name, or a short name. If it is a short name, the namespace context of
     * this builder will be used.
     * <br>
     * The name and namespace context rules are the same as the Avro schema JSON
     * specification.
     *
     * @param name The name of the schema
     * @return {@link GenericDefault}
     */
    public GenericDefault<R> type(String name) {
      return type(name, null);
    }

    /**
     * Final step in configuring this field, finalizing name, namespace, alias,
     * and order. Sets the field's type to the schema by name reference.
     * <br>
     * The name must correspond with a named schema that has already been
     * created in the context of this builder. The name may be a fully qualified
     * name, or a short name. If it is a full name, the namespace is ignored. If
     * it is a short name, the namespace provided is used. If the namespace
     * provided is null, the namespace context of this builder will be used.
     * <br>
     * The name and namespace context rules are the same as the Avro schema JSON
     * specification.
     *
     * @param name The name of the schema
     * @param namespace The namespace of the schema
     * @return {@link GenericDefault}
     */
    public GenericDefault<R> type(String name, String namespace) {
      Schema schema = names().get(name, namespace);
      return type(schema);
    }

    private FieldAssembler<R> completeField(Schema schema, Object defaultVal) {
      JsonNode defaultNode = toJsonNode(defaultVal);
      return completeField(schema, defaultNode);
    }

    private FieldAssembler<R> completeField(Schema schema) {
      return completeField(schema, null);
    }

    private FieldAssembler<R> completeField(Schema schema, JsonNode defaultVal) {
      Field field = new Field(name(), schema, doc(), defaultVal, order);
      addPropsTo(field);
      addAliasesTo(field);
      return fields.addField(field);
    }

    @Override
    protected FieldBuilder<R> self() {
      return this;
    }
  }

  /** Abstract base class for field defaults. */
  public static abstract class FieldDefault<R, S extends FieldDefault<R, S>> extends Completion<S> {
    private final FieldBuilder<R> field;
    private Schema schema;
    FieldDefault(FieldBuilder<R> field) {
      this.field = field;
    }

    /**
     * Completes this field with no default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> noDefault() {
      return field.completeField(schema);
    }

    private FieldAssembler<R> usingDefault(Object defaultVal) {
      return field.completeField(schema, defaultVal);
    }

    @Override
    final S complete(Schema schema) {
      this.schema = schema;
      return self();
    }

    abstract S self();
  }

  /** Choose whether to use a default value for the field or not. */
  public static class BooleanDefault<R> extends FieldDefault<R, BooleanDefault<R>> {
    private BooleanDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> booleanDefault(boolean defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final BooleanDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class IntDefault<R> extends FieldDefault<R, IntDefault<R>> {
    private IntDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> intDefault(int defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final IntDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class LongDefault<R> extends FieldDefault<R, LongDefault<R>> {
    private LongDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> longDefault(long defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final LongDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class FloatDefault<R> extends FieldDefault<R, FloatDefault<R>> {
    private FloatDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> floatDefault(float defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final FloatDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class DoubleDefault<R> extends FieldDefault<R, DoubleDefault<R>> {
    private DoubleDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> doubleDefault(double defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final DoubleDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class StringDefault<R> extends FieldDefault<R, StringDefault<R>> {
    private StringDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided. Cannot be null.
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> stringDefault(String defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final StringDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class BytesDefault<R> extends FieldDefault<R, BytesDefault<R>> {
    private BytesDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided, cannot be null
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> bytesDefault(byte[] defaultVal) {
      return super.usingDefault(ByteBuffer.wrap(defaultVal));
    }

    /**
     * Completes this field with the default value provided, cannot be null
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> bytesDefault(ByteBuffer defaultVal) {
      return super.usingDefault(defaultVal);
    }

    /**
     * Completes this field with the default value provided, cannot be null.
     * The string is interpreted as a byte[], with each character code point
     * value equalling the byte value, as in the Avro spec JSON default.
     * @param defaultVal Default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> bytesDefault(String defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final BytesDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class NullDefault<R> extends FieldDefault<R, NullDefault<R>> {
    private NullDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with a default value of null
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> nullDefault() {
      return super.usingDefault(null);
    }

    @Override
    final NullDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class MapDefault<R> extends FieldDefault<R, MapDefault<R>> {
    private MapDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided, cannot be null
     * @param <K> The key type
     * @param <V> The value type
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public final <K, V> FieldAssembler<R> mapDefault(Map<K, V> defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final MapDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class ArrayDefault<R> extends FieldDefault<R, ArrayDefault<R>> {
    private ArrayDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided, cannot be null
     *
     * @param <V> The field to construct
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public final <V> FieldAssembler<R> arrayDefault(List<V> defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final ArrayDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class FixedDefault<R> extends FieldDefault<R, FixedDefault<R>> {
    private FixedDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided, cannot be null
     *
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> fixedDefault(byte[] defaultVal) {
      return super.usingDefault(ByteBuffer.wrap(defaultVal));
    }

    /**
     * Completes this field with the default value provided, cannot be null
     *
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> fixedDefault(ByteBuffer defaultVal) {
      return super.usingDefault(defaultVal);
    }

    /** Completes this field with the default value provided, cannot be null.
     * The string is interpreted as a byte[], with each character code point
     * value equalling the byte value, as in the Avro spec JSON default.
     *
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> fixedDefault(String defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final FixedDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class EnumDefault<R> extends FieldDefault<R, EnumDefault<R>> {
    private EnumDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided, cannot be null
     *
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> enumDefault(String defaultVal) {
      return super.usingDefault(defaultVal);
    }

    /**
     * @return itself
     */
    @Override
    final EnumDefault<R> self() {
      return this;
    }
  }

  /** Choose whether to use a default value for the field or not. */
  public static class RecordDefault<R> extends FieldDefault<R, RecordDefault<R>> {
    private RecordDefault(FieldBuilder<R> field) {
      super(field);
    }

    /**
     * Completes this field with the default value provided, cannot be null
     *
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public final FieldAssembler<R> recordDefault(GenericRecord defaultVal) {
      return super.usingDefault(defaultVal);
    }

    @Override
    final RecordDefault<R> self() {
      return this;
    }
  }

  public final static class GenericDefault<R> {
    private final FieldBuilder<R> field;
    private final Schema schema;
    private GenericDefault(FieldBuilder<R> field, Schema schema) {
      this.field = field;
      this.schema = schema;
    }

    /**
     * Do not use a default value for this field.
     *
     * @return {@link FieldAssembler}
     */
    public FieldAssembler<R> noDefault() {
      return field.completeField(schema);
    }

    /**
     * Completes this field with the default value provided.
     * The value must conform to the schema of the field.
     *
     * @param defaultVal The default value
     * @return {@link FieldAssembler}
     */
    public FieldAssembler<R> withDefault(Object defaultVal) {
      return field.completeField(schema, defaultVal);
    }
  }

  /**
   * Completion<R> is for internal builder use, all subclasses are private.
   *
   * Completion is an object that takes a Schema and returns some result.
   */
  private abstract static class Completion<R> {
    abstract R complete(Schema schema);
  }

  private static class SchemaCompletion extends Completion<Schema> {
    @Override
    protected Schema complete(Schema schema) {
      return schema;
    }
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  private static class NullableCompletion<R> extends Completion<R> {
    private final Completion<R> context;
    private NullableCompletion(Completion<R> context) {
      this.context = context;
    }
    @Override
    protected R complete(Schema schema) {
      // wrap the schema as a union of the schema and null
      Schema nullable = Schema.createUnion(Arrays.asList(schema, NULL_SCHEMA));
      return context.complete(nullable);
    }
  }

  private static class OptionalCompletion<R> extends Completion<FieldAssembler<R>> {
    private final FieldBuilder<R> bldr;

    public OptionalCompletion(FieldBuilder<R> bldr) {
      this.bldr = bldr;
    }

    @Override
    protected FieldAssembler<R> complete(Schema schema) {
      // wrap the schema as a union of null and the schema
      Schema optional = Schema.createUnion(Arrays.asList(NULL_SCHEMA, schema));
      return bldr.completeField(optional, (Object)null);
    }
  }

  private abstract static class CompletionWrapper {
    abstract <R> Completion<R> wrap(Completion<R> completion);
  }

  private static final class NullableCompletionWrapper extends CompletionWrapper {
    @Override
    <R> Completion<R> wrap(Completion<R> completion) {
      return new NullableCompletion<>(completion);
    }
  }

  private static abstract class NestedCompletion<R> extends Completion<R> {
    private final Completion<R> context;
    private final PropBuilder<?> assembler;

    private NestedCompletion(PropBuilder<?> assembler, Completion<R> context) {
      this.context = context;
      this.assembler = assembler;
    }

    @Override
    protected final R complete(Schema schema) {
      Schema outer = outerSchema(schema);
      assembler.addPropsTo(outer);
      return context.complete(outer);
    }

    protected abstract Schema outerSchema(Schema inner);
  }

  private static class MapCompletion<R> extends NestedCompletion<R> {
    private MapCompletion(MapBuilder<R> assembler, Completion<R> context) {
      super(assembler, context);
    }

    @Override
    protected Schema outerSchema(Schema inner) {
      return Schema.createMap(inner);
    }
  }

  private static class ArrayCompletion<R> extends NestedCompletion<R> {
    private ArrayCompletion(ArrayBuilder<R> assembler, Completion<R> context) {
      super(assembler, context);
    }

    @Override
    protected Schema outerSchema(Schema inner) {
      return Schema.createArray(inner);
    }
  }

  private static class UnionCompletion<R> extends
      Completion<UnionAccumulator<R>> {
    private final Completion<R> context;
    private final NameContext names;
    private final List<Schema> schemas;

    private UnionCompletion(Completion<R> context, NameContext names, List<Schema> schemas) {
      this.context = context;
      this.names = names;
      this.schemas = schemas;
    }

    @Override
    protected UnionAccumulator<R> complete(Schema schema) {
      List<Schema> updated = new ArrayList<>(this.schemas);
      updated.add(schema);
      return new UnionAccumulator<>(context, names, updated);
    }
  }

  /** Accumulates all of the types in a union.  Add an additional type with
   * {@link #and()}.  Complete the union with {@link #endUnion()}
   */
  public static final class UnionAccumulator<R> {
    private final Completion<R> context;
    private final NameContext names;
    private final List<Schema> schemas;

    private UnionAccumulator(Completion<R> context, NameContext names, List<Schema> schemas) {
      this.context = context;
      this.names = names;
      this.schemas = schemas;
    }

    /**
     * Add an additional type to this union
     * @return {@link BaseTypeBuilder}
     */
    public BaseTypeBuilder<UnionAccumulator<R>> and() {
      return new UnionBuilder<>(context, names, schemas);
    }

    /**
     * Complete this union
     * @return {@link R}
     */
    public R endUnion() {
      Schema schema = Schema.createUnion(schemas);
      return context.complete(schema);
    }
  }

  private static void checkRequired(Object reference, String errorMessage) {
    if (reference == null) {
      throw new NullPointerException(errorMessage);
    }
  }

  // create default value JsonNodes from objects
  private static JsonNode toJsonNode(Object o) {
    try {
      String s;
      if (o instanceof ByteBuffer) {
        // special case since GenericData.toString() is incorrect for bytes
        // note that this does not handle the case of a default value with nested bytes
        ByteBuffer bytes = ((ByteBuffer) o);
        bytes.mark();
        byte[] data = new byte[bytes.remaining()];
        bytes.get(data);
        bytes.reset(); // put the buffer back the way we got it
        s = new String(data, "ISO-8859-1");
        char[] quoted = JsonStringEncoder.getInstance().quoteAsString(s);
        s = "\"" + new String(quoted) + "\"";
      } else if (o instanceof byte[]) {
        s = new String((byte[]) o, "ISO-8859-1");
        char[] quoted = JsonStringEncoder.getInstance().quoteAsString(s);
        s = '\"' + new String(quoted) + '\"';
      } else {
        s = GenericData.get().toString(o);
      }
      return new ObjectMapper().readTree(s);
    } catch (IOException e) {
      throw new SchemaBuilderException(e);
    }
  }
}
