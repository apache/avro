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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

/**
 * Utils for IDL
 */
public final class IdlUtils {
  static final ObjectMapper MAPPER;
  private static final Function<Schema.Field, JsonNode> DEFAULT_VALUE;
  private static final Pattern NEWLINE_PATTERN = Pattern.compile("(?U)\\R");
  private static final String NEWLINE = System.lineSeparator();
  private static final Set<String> KEYWORDS = unmodifiableSet(new HashSet<>(
      asList("array", "boolean", "bytes", "date", "decimal", "double", "enum", "error", "false", "fixed", "float",
          "idl", "import", "int", "local_timestamp_ms", "long", "map", "namespace", "null", "oneway", "protocol",
          "record", "schema", "string", "throws", "timestamp_ms", "time_ms", "true", "union", "uuid", "void")));
  private static final EnumSet<Schema.Type> NULLABLE_TYPES = EnumSet
      .complementOf(EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION));

  static {
    SimpleModule module = new SimpleModule();
    module.addSerializer(new StdSerializer<JsonProperties.Null>(JsonProperties.Null.class) {
      @Override
      public void serialize(JsonProperties.Null value, JsonGenerator gen, SerializerProvider provider)
          throws IOException {
        gen.writeNull();
      }
    });
    module.addSerializer(new StdSerializer<byte[]>(byte[].class) {
      @Override
      public void serialize(byte[] value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        MAPPER.writeValueAsString(new String(value, StandardCharsets.ISO_8859_1));
      }
    });

    ObjectMapper jsonMapper = getFieldValue(getField(Schema.class, "MAPPER"), null);
    MAPPER = jsonMapper.copy().registerModule(module).disable(DeserializationFeature.READ_ENUMS_USING_TO_STRING)
        .disable(SerializationFeature.WRITE_ENUMS_USING_INDEX, SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
        .enable(SerializationFeature.INDENT_OUTPUT).setDefaultPrettyPrinter(new MinimalPrettyPrinter() {
          @Override
          public void writeObjectEntrySeparator(JsonGenerator jg) throws IOException {
            jg.writeRaw(',');
            jg.writeRaw(' ');
          }

          @Override
          public void writeArrayValueSeparator(JsonGenerator jg) throws IOException {
            jg.writeRaw(',');
            jg.writeRaw(' ');
          }
        });

    java.lang.reflect.Field defaultValueField = getField(Schema.Field.class, "defaultValue");
    DEFAULT_VALUE = field -> getFieldValue(defaultValueField, field);
  }

  static java.lang.reflect.Field getField(Class<?> type, String name) {
    try {
      java.lang.reflect.Field field = type.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("Programmer error", e);
    }
  }

  static <T> T getFieldValue(java.lang.reflect.Field field, Object owner) {
    try {
      return (T) field.get(owner);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Programmer error", e);
    }
  }

  private IdlUtils() {
    // Utility class: do not instantiate.
  }

  /**
   * Calls the given callable, wrapping any checked exception in an
   * {@link AvroRuntimeException}.
   *
   * @param callable the callable to call
   * @return the result of the callable
   */
  public static <T> T uncheckExceptions(Callable<T> callable) {
    try {
      return requireNonNull(callable).call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new AvroRuntimeException(e.getMessage(), e);
    }
  }

  public static void writeIdlSchema(Writer writer, Schema schema) throws IOException {
    writeIdlSchemas(writer, schema.getNamespace(), singleton(schema));
  }

  public static void writeIdlSchemas(Writer writer, String namespace, Collection<Schema> schemas) throws IOException {
    if (schemas.isEmpty()) {
      throw new IllegalArgumentException("Cannot write 0 schemas");
    }
    if (namespace != null) {
      writer.append("namespace ");
      writer.append(namespace);
      writer.append(";");
      writer.append(NEWLINE);
      writer.append(NEWLINE);
    }

    Set<String> alreadyDeclared = new HashSet<>(4);
    Set<Schema> toDeclare = new LinkedHashSet<>();
    if (schemas.size() == 1) {
      Schema schema = schemas.iterator().next();
      writer.append("schema ");
      // Note: as alreadyDeclared is empty, writeFieldSchema adds schema to toDeclare
      writeFieldSchema(schema, writer, alreadyDeclared, toDeclare, namespace);
      writer.append(";");
      writer.append(NEWLINE);
      writer.append(NEWLINE);
    } else {
      toDeclare.addAll(schemas);
    }

    while (!toDeclare.isEmpty()) {
      if (!alreadyDeclared.isEmpty()) {
        writer.append(NEWLINE);
      }
      Iterator<Schema> iterator = toDeclare.iterator();
      Schema s = iterator.next();
      iterator.remove();
      writeSchema(s, false, writer, namespace, alreadyDeclared, toDeclare);
    }
  }

  public static void writeIdlProtocol(Writer writer, Protocol protocol) throws IOException {
    final String protocolFullName = protocol.getName();
    final int lastDotPos = protocolFullName.lastIndexOf(".");
    final String protocolNameSpace;
    if (lastDotPos < 0) {
      protocolNameSpace = protocol.getNamespace();
    } else if (lastDotPos > 0) {
      protocolNameSpace = protocolFullName.substring(0, lastDotPos);
    } else {
      protocolNameSpace = null;
    }
    writeIdlProtocol(writer, protocol, protocolNameSpace, protocolFullName.substring(lastDotPos + 1),
        protocol.getTypes(), protocol.getMessages().values());
  }

  public static void writeIdlProtocol(Writer writer, Schema schema) throws IOException {
    final JsonProperties emptyProperties = Schema.create(Schema.Type.NULL);
    writeIdlProtocol(writer, emptyProperties, schema.getNamespace(), "Protocol", singletonList(schema), emptyList());
  }

  public static void writeIdlProtocol(Writer writer, JsonProperties properties, String protocolNameSpace,
      String protocolName, Collection<Schema> schemas, Collection<Protocol.Message> messages) throws IOException {
    if (protocolNameSpace != null) {
      writer.append("@namespace(\"").append(protocolNameSpace).append("\")").append(NEWLINE);
    }
    writeJsonProperties(properties, singleton("namespace"), writer, "");
    writer.append("protocol ").append(requireNonNull(safeName(protocolName))).append(" {").append(NEWLINE);

    Set<String> alreadyDeclared = new HashSet<>(4);
    Set<Schema> toDeclare = new LinkedHashSet<>(schemas);
    boolean first = true;
    while (!toDeclare.isEmpty()) {
      if (!first) {
        writer.append(NEWLINE);
      }
      Iterator<Schema> iterator = toDeclare.iterator();
      Schema schema = iterator.next();
      iterator.remove();
      writeSchema(schema, true, writer, protocolNameSpace, alreadyDeclared, toDeclare);
      first = false;
    }
    if (!schemas.isEmpty() && !messages.isEmpty()) {
      writer.append(NEWLINE);
    }
    for (Protocol.Message message : messages) {
      writeMessage(message, writer, protocolNameSpace, alreadyDeclared);
    }
    writer.append("}").append(NEWLINE);
  }

  private static String safeName(String name) {
    if (KEYWORDS.contains(name)) {
      return String.format("`%s`", name);
    }
    return name;
  }

  private static void writeSchema(Schema schema, boolean insideProtocol, Writer writer, String defaultNamespace,
      Set<String> alreadyDeclared, Set<Schema> toDeclare) throws IOException {
    String indent = insideProtocol ? "    " : "";
    Schema.Type type = schema.getType();
    writeSchemaAttributes(indent, schema, writer);
    String namespace = schema.getNamespace(); // Fails for unnamed schema types (other types than record, enum & fixed)
    if (!Objects.equals(namespace, defaultNamespace)) {
      writer.append(indent).append("@namespace(\"").append(namespace).append("\")").append(NEWLINE);
    }
    Set<String> schemaAliases = schema.getAliases();
    if (!schemaAliases.isEmpty()) {
      writer.append(indent).append("@aliases(").append(MAPPER.writeValueAsString(schemaAliases)).append(")")
          .append(NEWLINE);
    }
    String schemaName = safeName(schema.getName());
    if (type == Schema.Type.RECORD) {
      String declarationType = schema.isError() ? "error" : "record";
      writer.append(indent).append("").append(declarationType).append(" ").append(schemaName).append(" {")
          .append(NEWLINE);
      alreadyDeclared.add(schema.getFullName());
      for (Schema.Field field : schema.getFields()) {
        writeField(schema.getNamespace(), field, writer, alreadyDeclared, toDeclare,
            insideProtocol ? FieldIndent.INSIDE_PROTOCOL : FieldIndent.TOPLEVEL_SCHEMA);
        writer.append(";").append(NEWLINE);
      }
      writer.append(indent).append("}").append(NEWLINE);
    } else if (type == Schema.Type.ENUM) {
      writer.append(indent).append("enum ").append(schemaName).append(" {").append(NEWLINE);
      alreadyDeclared.add(schema.getFullName());
      Iterator<String> i = schema.getEnumSymbols().iterator();
      if (i.hasNext()) {
        writer.append(indent).append("    ").append(i.next());
        while (i.hasNext()) {
          writer.append(", ");
          writer.append(i.next());
        }
      } else {
        throw new AvroRuntimeException("Enum schema must have at least a symbol " + schema);
      }
      writer.append(NEWLINE).append(indent).append("}").append(NEWLINE);
    } else /* (type == Schema.Type.FIXED) */ {
      writer.append(indent).append("fixed ").append(schemaName).append('(')
          .append(Integer.toString(schema.getFixedSize())).append(");").append(NEWLINE);
      alreadyDeclared.add(schema.getFullName());
    }
  }

  private static void writeField(String namespace, Schema.Field field, Writer writer, Set<String> alreadyDeclared,
      Set<Schema> toDeclare, FieldIndent fieldIndent) throws IOException {
    // Note: indentField must not be NONE if any field of the containing
    // record/method has documentation
    switch (fieldIndent) {
    case TOPLEVEL_SCHEMA:
      writeDocumentation(writer, "    ", field.doc());
      writer.append("    ");
      break;
    case INSIDE_PROTOCOL:
      writeDocumentation(writer, "        ", field.doc());
      writer.append("        ");
      break;
    }
    writeFieldSchema(field.schema(), writer, alreadyDeclared, toDeclare, namespace);
    writer.append(' ');
    Set<String> fieldAliases = field.aliases();
    if (!fieldAliases.isEmpty()) {
      writer.append("@aliases(").append(MAPPER.writeValueAsString(fieldAliases)).append(") ");
    }
    Schema.Field.Order order = field.order();
    if (order != Schema.Field.Order.ASCENDING) {
      writer.append("@order(\"").append(order.name()).append("\") ");
    }
    writeJsonProperties(field, writer, null);
    writer.append(field.name());
    JsonNode defaultValue = DEFAULT_VALUE.apply(field);
    if (defaultValue != null) {
      Object datum = field.defaultVal();
      writer.append(" = ").append(MAPPER.writeValueAsString(datum));
    }
  }

  private static void writeDocumentation(Writer writer, String indent, String doc) throws IOException {
    if (doc == null || doc.trim().isEmpty()) {
      return;
    }
    writer.append(formatDocumentationComment(indent, doc));
  }

  private static String formatDocumentationComment(String indent, String doc) {
    assert !doc.trim().isEmpty() : "There must be documentation to format!";

    StringBuffer buffer = new StringBuffer();
    buffer.append(indent).append("/** ");
    boolean foundMatch = false;
    final Matcher matcher = NEWLINE_PATTERN.matcher(doc);
    final String newlinePlusIndent = NEWLINE + indent + " * ";
    while (matcher.find()) {
      if (!foundMatch) {
        buffer.append(newlinePlusIndent);
        foundMatch = true;
      }
      matcher.appendReplacement(buffer, newlinePlusIndent);
    }
    if (foundMatch) {
      matcher.appendTail(buffer);
      buffer.append(NEWLINE).append(indent).append(" */").append(NEWLINE);
    } else {
      buffer.append(doc).append(" */").append(NEWLINE);
    }
    return buffer.toString();
  }

  private static void writeFieldSchema(Schema schema, Writer writer, Set<String> alreadyDeclared, Set<Schema> toDeclare,
      String recordNameSpace) throws IOException {
    Schema.Type type = schema.getType();
    if (type == Schema.Type.RECORD || type == Schema.Type.ENUM || type == Schema.Type.FIXED) {
      if (Objects.equals(recordNameSpace, schema.getNamespace())) {
        writer.append(schema.getName());
      } else {
        writer.append(schema.getFullName());
      }
      if (!alreadyDeclared.contains(schema.getFullName())) {
        toDeclare.add(schema);
      }
    } else if (type == Schema.Type.ARRAY) {
      writeJsonProperties(schema, writer, null);
      writer.append("array<");
      writeFieldSchema(schema.getElementType(), writer, alreadyDeclared, toDeclare, recordNameSpace);
      writer.append('>');
    } else if (type == Schema.Type.MAP) {
      writeJsonProperties(schema, writer, null);
      writer.append("map<");
      writeFieldSchema(schema.getValueType(), writer, alreadyDeclared, toDeclare, recordNameSpace);
      writer.append('>');
    } else if (type == Schema.Type.UNION) {
      // Note: unions cannot have properties
      Schema schemaForNullableSyntax = getNullableUnionType(schema);
      if (schemaForNullableSyntax != null) {
        writeFieldSchema(schemaForNullableSyntax, writer, alreadyDeclared, toDeclare, recordNameSpace);
        writer.append('?');
      } else {
        writer.append("union{");
        List<Schema> types = schema.getTypes();
        Iterator<Schema> iterator = types.iterator();
        if (iterator.hasNext()) {
          writeFieldSchema(iterator.next(), writer, alreadyDeclared, toDeclare, recordNameSpace);
          while (iterator.hasNext()) {
            writer.append(", ");
            writeFieldSchema(iterator.next(), writer, alreadyDeclared, toDeclare, recordNameSpace);
          }
        } else {
          throw new AvroRuntimeException("Union schemas must have member types " + schema);
        }
        writer.append('}');
      }
    } else {
      Set<String> propertiesToSkip = new HashSet<>();
      String typeName;
      if (schema.getLogicalType() == null) {
        typeName = schema.getName();
      } else {
        String logicalName = schema.getLogicalType().getName();
        switch (logicalName) {
        // TODO: Use constants from org.apache.avro.LogicalTypes
        case "date":
        case "time-millis":
        case "timestamp-millis":
          propertiesToSkip.add("logicalType");
          typeName = logicalName.replace("-millis", "_ms");
          break;
        case "decimal":
          propertiesToSkip.addAll(asList("logicalType", "precision", "scale"));
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) schema.getLogicalType();
          typeName = String.format("decimal(%d,%d)", decimal.getPrecision(), decimal.getScale());
          break;
        default:
          propertiesToSkip = Collections.emptySet();
          typeName = schema.getName();
          break;
        }
      }
      writeJsonProperties(schema, propertiesToSkip, writer, null);
      writer.append(typeName);
    }
  }

  /**
   * Get the type from a nullable 2-type union if that type is eligible for the
   * '?'-syntax.
   *
   * @param unionSchema a union schema
   * @return the non-null schema in a nullable 2-type union iff not a container
   */
  private static Schema getNullableUnionType(Schema unionSchema) {
    List<Schema> types = unionSchema.getTypes();
    if (unionSchema.isNullable() && types.size() == 2) {
      Schema nonNullSchema = !types.get(0).isNullable() ? types.get(0) : types.get(1);
      if (NULLABLE_TYPES.contains(nonNullSchema.getType())) {
        return nonNullSchema;
      }
    }
    return null;
  }

  private static void writeSchemaAttributes(String indent, Schema schema, Writer writer) throws IOException {
    writeDocumentation(writer, indent, schema.getDoc());
    writeJsonProperties(schema, writer, indent);
  }

  private static void writeJsonProperties(JsonProperties props, Writer writer, String indent) throws IOException {
    writeJsonProperties(props, Collections.emptySet(), writer, indent);
  }

  private static void writeJsonProperties(JsonProperties props, Set<String> propertiesToSkip, Writer writer,
      String indent) throws IOException {
    Map<String, Object> objectProps = props.getObjectProps();
    for (Map.Entry<String, Object> entry : objectProps.entrySet()) {
      if (propertiesToSkip.contains(entry.getKey())) {
        continue;
      }
      if (indent != null) {
        writer.append(indent);
      }
      writer.append('@').append(entry.getKey()).append('(');
      writer.append(MAPPER.writeValueAsString(entry.getValue())).append(')');
      writer.append(indent == null ? ' ' : '\n');
    }
  }

  private static void writeMessage(Protocol.Message message, Writer writer, String protocolNameSpace,
      Set<String> alreadyDeclared) throws IOException {
    writeMessageAttributes(message, writer);
    final Set<Schema> toDeclare = Collections.unmodifiableSet(new HashSet<>()); // Crash if a type hasn't been declared
    // yet.
    writer.append("    ");
    writeFieldSchema(message.getResponse(), writer, alreadyDeclared, toDeclare, protocolNameSpace);
    writer.append(' ');
    writer.append(message.getName());

    Schema request = message.getRequest(); // MUST be a record type
    boolean indentParameters = request.getFields().stream()
        .anyMatch(field -> field.doc() != null && !field.doc().trim().isEmpty());
    writer.append('(');
    if (indentParameters) {
      writer.append("\n");
    }

    boolean first = true;
    for (Schema.Field field : request.getFields()) {
      if (first) {
        first = false;
      } else if (indentParameters) {
        writer.append(",\n");
      } else {
        writer.append(", ");
      }
      writeField(protocolNameSpace, field, writer, alreadyDeclared, toDeclare,
          indentParameters ? FieldIndent.INSIDE_PROTOCOL : FieldIndent.NONE);
    }
    if (indentParameters) {
      writer.append("\n    ");
    }
    writer.append(')');

    if (message.isOneWay()) {
      writer.append(" oneway;\n");
    } else {
      first = true;
      // MUST be a union of error types
      for (Schema error : message.getErrors().getTypes()) {
        if (error.getType() == Schema.Type.STRING) {
          continue; // Skip system error type
        }
        if (first) {
          first = false;
          writer.append(" throws ");
        } else {
          writer.append(", ");
        }
        if (Objects.equals(protocolNameSpace, error.getNamespace())) {
          writer.append(error.getName());
        } else {
          writer.append(error.getFullName());
        }
      }
      writer.append(";\n");
    }
  }

  private static void writeMessageAttributes(Protocol.Message message, Writer writer) throws IOException {
    writeDocumentation(writer, "    ", message.getDoc());
    writeJsonProperties(message, writer, "    ");
  }

  private enum FieldIndent {
    NONE, TOPLEVEL_SCHEMA, INSIDE_PROTOCOL
  }
}
