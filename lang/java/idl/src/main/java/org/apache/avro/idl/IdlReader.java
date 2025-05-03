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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.apache.avro.JsonProperties;
import org.apache.avro.JsonSchemaParser;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.ParseContext;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.idl.IdlParser.ArrayTypeContext;
import org.apache.avro.idl.IdlParser.EnumDeclarationContext;
import org.apache.avro.idl.IdlParser.EnumSymbolContext;
import org.apache.avro.idl.IdlParser.FieldDeclarationContext;
import org.apache.avro.idl.IdlParser.FixedDeclarationContext;
import org.apache.avro.idl.IdlParser.FormalParameterContext;
import org.apache.avro.idl.IdlParser.FullTypeContext;
import org.apache.avro.idl.IdlParser.IdentifierContext;
import org.apache.avro.idl.IdlParser.IdlFileContext;
import org.apache.avro.idl.IdlParser.ImportStatementContext;
import org.apache.avro.idl.IdlParser.JsonArrayContext;
import org.apache.avro.idl.IdlParser.JsonLiteralContext;
import org.apache.avro.idl.IdlParser.JsonObjectContext;
import org.apache.avro.idl.IdlParser.JsonPairContext;
import org.apache.avro.idl.IdlParser.JsonValueContext;
import org.apache.avro.idl.IdlParser.MapTypeContext;
import org.apache.avro.idl.IdlParser.MessageDeclarationContext;
import org.apache.avro.idl.IdlParser.NamespaceDeclarationContext;
import org.apache.avro.idl.IdlParser.NullableTypeContext;
import org.apache.avro.idl.IdlParser.PrimitiveTypeContext;
import org.apache.avro.idl.IdlParser.ProtocolDeclarationBodyContext;
import org.apache.avro.idl.IdlParser.ProtocolDeclarationContext;
import org.apache.avro.idl.IdlParser.RecordBodyContext;
import org.apache.avro.idl.IdlParser.RecordDeclarationContext;
import org.apache.avro.idl.IdlParser.ResultTypeContext;
import org.apache.avro.idl.IdlParser.SchemaPropertyContext;
import org.apache.avro.idl.IdlParser.UnionTypeContext;
import org.apache.avro.idl.IdlParser.VariableDeclarationContext;
import org.apache.avro.util.SchemaResolver;
import org.apache.avro.util.UtfTextUtils;
import org.apache.avro.util.internal.Accessor;
import org.apache.commons.text.StringEscapeUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;

public class IdlReader {
  /**
   * Simple error listener. Throws a runtime exception because ANTLR does not give
   * easy access to the (reasonably readable) error message elsewhere.
   */
  private static final BaseErrorListener SIMPLE_AVRO_ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
        String msg, RecognitionException e) {
      throw new SchemaParseException("line " + line + ":" + charPositionInLine + " " + msg);
    }
  };
  private static final String OPTIONAL_NULLABLE_TYPE_PROPERTY = "org.apache.avro.idl.Idl.NullableType.optional";
  /**
   * Pattern to match the common whitespace indents in a multi-line String.
   * Doesn't match a single-line String, fully matches any multi-line String.
   * <p>
   * To use: match on a {@link String#trim() trimmed} String, and then replace all
   * newlines followed by the group "indent" with a newline.
   */
  private static final Pattern WS_INDENT = Pattern.compile("(?U).*\\R(?<indent>\\h*).*(?:\\R\\k<indent>.*)*");
  /**
   * Pattern to match the whitespace indents plus common stars (1 or 2) in a
   * multi-line String. If a String fully matches, replace all occurrences of a
   * newline followed by whitespace and then the group "stars" with a newline.
   * <p>
   * Note: partial matches are invalid.
   */
  private static final Pattern STAR_INDENT = Pattern.compile("(?U)(?<stars>\\*{1,2}).*(?:\\R\\h*\\k<stars>.*)*");
  /**
   * Predicate to check for valid names. Should probably be delegated to the
   * Schema class.
   */
  private static final Predicate<String> VALID_NAME = Pattern
      .compile("[_\\p{L}][_\\p{LD}]*", Pattern.UNICODE_CHARACTER_CLASS | Pattern.UNICODE_CASE | Pattern.CANON_EQ)
      .asPredicate();

  private static final Set<String> INVALID_TYPE_NAMES = new HashSet<>(Arrays.asList("boolean", "int", "long", "float",
      "double", "bytes", "string", "null", "date", "time_ms", "timestamp_ms", "localtimestamp_ms", "uuid"));
  private static final String CLASSPATH_SCHEME = "classpath";
  private static final Set<Schema.Type> NAMED_SCHEMA_TYPES = EnumSet.of(Schema.Type.RECORD, Schema.Type.ENUM,
      Schema.Type.FIXED);

  private final Set<URI> readLocations;
  private final ParseContext parseContext;

  public IdlReader() {
    this(new ParseContext());
  }

  public IdlReader(ParseContext parseContext) {
    readLocations = new HashSet<>();
    this.parseContext = parseContext;
  }

  private Schema namedSchemaOrUnresolved(String fullName) {
    return parseContext.find(fullName, null);
  }

  public IdlFile parse(Path location) throws IOException {
    return parse(location.toUri());
  }

  IdlFile parse(URI location) throws IOException {
    readLocations.add(location);
    URI inputDir = location;
    if ("jar".equals(location.getScheme())) {
      String jarUriAsString = location.toString();
      String pathFromJarRoot = jarUriAsString.substring(jarUriAsString.indexOf("!/") + 2);
      inputDir = URI.create(CLASSPATH_SCHEME + ":/" + pathFromJarRoot);
    }
    inputDir = inputDir.resolve(".");

    try (InputStream stream = location.toURL().openStream()) {
      String inputString = UtfTextUtils.readAllBytes(stream, null);
      return parse(inputDir, CharStreams.fromString(inputString));
    }
  }

  /**
   * Parse an IDL file from a string, using the given directory for imports.
   */
  public IdlFile parse(URI directory, CharSequence source) throws IOException {
    return parse(directory, CharStreams.fromString(source.toString()));
  }

  /**
   * Parse an IDL file from a stream. This method cannot handle imports.
   */
  public IdlFile parse(InputStream stream) throws IOException {
    return parse(null, CharStreams.fromStream(stream, StandardCharsets.UTF_8));
  }

  private IdlFile parse(URI inputDir, CharStream charStream) {
    IdlLexer lexer = new IdlLexer(charStream);
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);

    IdlParserListener parseListener = new IdlParserListener(inputDir, tokenStream);

    IdlParser parser = new IdlParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(SIMPLE_AVRO_ERROR_LISTENER);
    parser.addParseListener(parseListener);
    parser.setTrace(false);
    parser.setBuildParseTree(false);

    try {
      // Trigger parsing.
      parser.idlFile();
    } catch (SchemaParseException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new SchemaParseException(e);
    }

    return parseListener.getIdlFile();
  }

  /* Package private to facilitate testing */
  static String stripIndents(String docComment) {
    Matcher starMatcher = STAR_INDENT.matcher(docComment);
    if (starMatcher.matches()) {
      return docComment.replaceAll("(?U)(?:^|(\\R)\\h*)\\Q" + starMatcher.group("stars") + "\\E\\h?", "$1");
    }

    Matcher whitespaceMatcher = WS_INDENT.matcher(docComment);
    if (whitespaceMatcher.matches()) {
      return docComment.replaceAll("(?U)(\\R)" + whitespaceMatcher.group("indent"), "$1");
    }

    return docComment;
  }

  private static SchemaParseException error(String message, Token token) {
    return error(message, token, null);
  }

  private static SchemaParseException error(String message, Token token, Throwable cause) {
    SchemaParseException exception = new SchemaParseException(
        message + ", at line " + token.getLine() + ", column " + token.getCharPositionInLine());
    if (cause != null) {
      exception.initCause(cause);
    }
    return exception;
  }

  private class IdlParserListener extends IdlBaseListener {
    private final URI inputDir;
    private final CommonTokenStream tokenStream;
    private int hiddenTokensProcessedIndex;
    private final List<String> warnings;

    private IdlFile result;
    private Schema mainSchema;
    private Protocol protocol;
    private final Deque<String> namespaces;
    private final List<String> enumSymbols;
    private String enumDefaultSymbol;
    private Schema schema;
    private String defaultVariableDocComment;
    private final List<Schema.Field> fields;
    private final Deque<Schema> typeStack;
    private final Deque<JsonNode> jsonValues;
    private final Deque<SchemaProperties> propertiesStack;
    private String messageDocComment;

    public IdlParserListener(URI inputDir, CommonTokenStream tokenStream) {
      this.inputDir = inputDir;
      this.tokenStream = tokenStream;
      hiddenTokensProcessedIndex = -1;
      warnings = new ArrayList<>();

      result = null;
      mainSchema = null;
      protocol = null;
      namespaces = new ArrayDeque<>();
      enumSymbols = new ArrayList<>();
      enumDefaultSymbol = null;
      schema = null;
      defaultVariableDocComment = null;
      fields = new ArrayList<>();
      typeStack = new ArrayDeque<>();
      propertiesStack = new ArrayDeque<>();
      jsonValues = new ArrayDeque<>();
      messageDocComment = null;
    }

    public IdlFile getIdlFile() {
      return result;
    }

    private String getDocComment(ParserRuleContext ctx) {
      int newHiddenTokensProcessedIndex = ctx.start.getTokenIndex();
      List<Token> docCommentTokens = tokenStream.getHiddenTokensToLeft(newHiddenTokensProcessedIndex, -1);
      int searchEndIndex = newHiddenTokensProcessedIndex;

      Token docCommentToken = null;
      if (docCommentTokens != null) {
        // There's at least one element
        docCommentToken = docCommentTokens.get(docCommentTokens.size() - 1);
        searchEndIndex = docCommentToken.getTokenIndex() - 1;
      }

      Set<Integer> allHiddenTokens = singleton(IdlParser.DocComment);
      if (searchEndIndex >= 0) {
        List<Token> hiddenTokens = tokenStream.getTokens(hiddenTokensProcessedIndex + 1, searchEndIndex,
            allHiddenTokens);
        if (hiddenTokens != null) {
          for (Token token : hiddenTokens) {
            warnings.add(String.format(
                "Line %d, char %d: Ignoring out-of-place documentation comment.%n"
                    + "Did you mean to use a multiline comment ( /* ... */ ) instead?",
                token.getLine(), token.getCharPositionInLine() + 1));
          }
        }
      }
      hiddenTokensProcessedIndex = newHiddenTokensProcessedIndex;

      if (docCommentToken == null) {
        return null;
      }
      String comment = docCommentToken.getText();
      String text = comment.substring(3, comment.length() - 2); // Strip /** & */
      return stripIndents(text.trim());
    }

    private void pushNamespace(String namespace) {
      namespaces.push(namespace == null ? "" : namespace);
    }

    private String currentNamespace() {
      String namespace = namespaces.peek();
      return namespace == null || namespace.isEmpty() ? null : namespace;
    }

    private void popNamespace() {
      namespaces.pop();
    }

    @Override
    public void exitIdlFile(IdlFileContext ctx) {
      if (protocol == null) {
        result = new IdlFile(mainSchema, parseContext, warnings);
      } else {
        result = new IdlFile(protocol, parseContext, warnings);
      }
    }

    @Override
    public void enterProtocolDeclaration(ProtocolDeclarationContext ctx) {
      propertiesStack.push(new SchemaProperties(null, true, false, false));
    }

    @Override
    public void enterProtocolDeclarationBody(ProtocolDeclarationBodyContext ctx) {
      ProtocolDeclarationContext protocolCtx = (ProtocolDeclarationContext) ctx.parent;
      SchemaProperties properties = propertiesStack.pop();
      String protocolIdentifier = identifier(protocolCtx.name);
      pushNamespace(namespace(protocolIdentifier, properties.namespace()));

      String protocolName = name(protocolIdentifier);
      String docComment = getDocComment(protocolCtx);
      String protocolNamespace = currentNamespace();
      protocol = properties.copyProperties(new Protocol(protocolName, docComment, protocolNamespace));
    }

    @Override
    public void exitProtocolDeclaration(ProtocolDeclarationContext ctx) {
      if (protocol != null) {
        parseContext.commit();
        protocol.setTypes(parseContext.resolveAllSchemas());
      }
      if (!namespaces.isEmpty())
        popNamespace();
    }

    @Override
    public void exitNamespaceDeclaration(NamespaceDeclarationContext ctx) {
      pushNamespace(namespace("", identifier(ctx.namespace)));
    }

    @Override
    public void exitMainSchemaDeclaration(IdlParser.MainSchemaDeclarationContext ctx) {
      mainSchema = typeStack.pop();

      if (NAMED_SCHEMA_TYPES.contains(mainSchema.getType()) && mainSchema.getFullName() != null) {
        parseContext.put(mainSchema);
      }
      assert typeStack.isEmpty();
    }

    @Override
    public void enterSchemaProperty(SchemaPropertyContext ctx) {
      assert jsonValues.isEmpty();
    }

    @Override
    public void exitSchemaProperty(SchemaPropertyContext ctx) {
      String name = identifier(ctx.name);
      JsonNode value = jsonValues.pop();
      Token firstToken = ctx.value.start;

      propertiesStack.element().addProperty(name, value, firstToken);
      super.exitSchemaProperty(ctx);
    }

    @Override
    public void exitImportStatement(ImportStatementContext importContext) {
      String importFile = getString(importContext.location);
      try {
        URI importLocation = findImport(importFile);
        if (!readLocations.add(importLocation)) {
          // Already imported
          return;
        }
        switch (importContext.importType.getType()) {
        case IdlParser.IDL:
          // Note that the parse(URI) method uses the same known schema collection
          IdlFile idlFile = parse(importLocation);
          if (protocol != null && idlFile.getProtocol() != null) {
            protocol.getMessages().putAll(idlFile.getProtocol().getMessages());
          }
          warnings.addAll(idlFile.getWarnings(importFile));
          break;
        case IdlParser.Protocol:
          try (InputStream stream = importLocation.toURL().openStream()) {
            Protocol importProtocol = Protocol.parse(stream);
            for (Schema s : importProtocol.getTypes()) {
              parseContext.put(s);
            }
            if (protocol != null) {
              protocol.getMessages().putAll(importProtocol.getMessages());
            }
          }
          break;
        case IdlParser.Schema:
          try (InputStream stream = importLocation.toURL().openStream()) {
            JsonSchemaParser parser = new JsonSchemaParser();
            parser.parse(parseContext, importLocation.resolve("."), UtfTextUtils.readAllBytes(stream, null));
          }
          break;
        }
      } catch (IOException e) {
        throw error("Error importing " + importFile + ": " + e, importContext.location, e);
      }
    }

    /**
     * Best effort guess at the import file location. For locations inside jar
     * files, this may result in non-existing URLs.
     */
    private URI findImport(String importFile) throws IOException {
      URI importLocation = inputDir.resolve(importFile);
      String importLocationScheme = importLocation.getScheme();

      if (CLASSPATH_SCHEME.equals(importLocationScheme)) {
        String resourceName = importLocation.getSchemeSpecificPart().substring(1);
        URI resourceLocation = findResource(resourceName);
        if (resourceLocation != null) {
          return resourceLocation;
        }
      }

      if ("file".equals(importLocationScheme) && Files.exists(Paths.get(importLocation))) {
        return importLocation;
      }

      // The importFile doesn't exist as file relative to the current file. Try to
      // load it from the classpath.
      URI resourceLocation = findResource(importFile);
      if (resourceLocation != null) {
        return resourceLocation;
      }

      // Cannot find the import.
      throw new FileNotFoundException(importFile);
    }

    private URI findResource(String resourceName) {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      URL resourceLocation;
      if (classLoader == null) {
        resourceLocation = ClassLoader.getSystemResource(resourceName);
      } else {
        resourceLocation = classLoader.getResource(resourceName);
      }
      return resourceLocation == null ? null : URI.create(resourceLocation.toExternalForm());
    }

    @Override
    public void enterFixedDeclaration(FixedDeclarationContext ctx) {
      propertiesStack.push(new SchemaProperties(currentNamespace(), true, true, false));
    }

    @Override
    public void exitFixedDeclaration(FixedDeclarationContext ctx) {
      SchemaProperties properties = propertiesStack.pop();

      String doc = getDocComment(ctx);
      String identifier = identifier(ctx.name);
      String name = name(identifier);
      String space = namespace(identifier, properties.namespace());
      int size = Integer.decode(ctx.size.getText());
      Schema schema = Schema.createFixed(name, doc, space, size);
      properties.copyAliases(schema::addAlias);
      properties.copyProperties(schema);
      parseContext.put(schema);
    }

    @Override
    public void enterEnumDeclaration(EnumDeclarationContext ctx) {
      assert enumSymbols.isEmpty();
      assert enumDefaultSymbol == null;
      propertiesStack.push(new SchemaProperties(currentNamespace(), true, true, false));
    }

    @Override
    public void exitEnumDeclaration(EnumDeclarationContext ctx) {
      String doc = getDocComment(ctx);
      SchemaProperties properties = propertiesStack.pop();
      String identifier = identifier(ctx.name);
      String name = name(identifier);
      String space = namespace(identifier, properties.namespace());

      Schema schema = Schema.createEnum(name, doc, space, new ArrayList<>(enumSymbols), enumDefaultSymbol);
      properties.copyAliases(schema::addAlias);
      properties.copyProperties(schema);
      enumSymbols.clear();
      enumDefaultSymbol = null;

      parseContext.put(schema);
    }

    @Override
    public void enterEnumSymbol(EnumSymbolContext ctx) {
      propertiesStack.push(new SchemaProperties(null, false, false, false));
    }

    @Override
    public void exitEnumSymbol(EnumSymbolContext ctx) {
      // TODO: implement doc comment & properties for enum symbols.
      propertiesStack.pop();

      enumSymbols.add(identifier(ctx.name));
    }

    @Override
    public void exitEnumDefault(IdlParser.EnumDefaultContext ctx) {
      enumDefaultSymbol = identifier(ctx.defaultSymbolName);
    }

    @Override
    public void enterRecordDeclaration(RecordDeclarationContext ctx) {
      assert schema == null;
      assert fields.isEmpty();

      propertiesStack.push(new SchemaProperties(currentNamespace(), true, true, false));
    }

    @Override
    public void enterRecordBody(RecordBodyContext ctx) {
      assert fields.isEmpty();

      RecordDeclarationContext recordCtx = (RecordDeclarationContext) ctx.parent;

      SchemaProperties properties = propertiesStack.pop();

      String doc = getDocComment(recordCtx);
      String identifier = identifier(recordCtx.name);
      String name = name(identifier);
      pushNamespace(namespace(identifier, properties.namespace()));
      boolean isError = recordCtx.recordType.getType() == IdlParser.Error;
      schema = Schema.createRecord(name, doc, currentNamespace(), isError);
      properties.copyAliases(schema::addAlias);
      properties.copyProperties(schema);
    }

    @Override
    public void exitRecordDeclaration(RecordDeclarationContext ctx) {
      schema.setFields(fields);
      fields.clear();
      parseContext.put(schema);
      schema = null;

      popNamespace();
    }

    @Override
    public void enterFieldDeclaration(FieldDeclarationContext ctx) {
      assert typeStack.isEmpty();
      defaultVariableDocComment = getDocComment(ctx);
    }

    @Override
    public void exitFieldDeclaration(FieldDeclarationContext ctx) {
      typeStack.pop();
      defaultVariableDocComment = null;
    }

    @Override
    public void enterVariableDeclaration(VariableDeclarationContext ctx) {
      assert jsonValues.isEmpty();
      propertiesStack.push(new SchemaProperties(currentNamespace(), false, true, true));
    }

    @Override
    public void exitVariableDeclaration(VariableDeclarationContext ctx) {
      String doc = Optional.ofNullable(getDocComment(ctx)).orElse(defaultVariableDocComment);
      String fieldName = identifier(ctx.fieldName);

      JsonNode defaultValue = jsonValues.poll();
      Schema type = typeStack.element();
      JsonNode fieldDefault = fixDefaultValue(defaultValue, type);
      Schema fieldType = fixOptionalSchema(type, fieldDefault);

      SchemaProperties properties = propertiesStack.pop();

      boolean validate = SchemaResolver.isFullyResolvedSchema(fieldType);
      Schema.Field field = Accessor.createField(fieldName, fieldType, doc, fieldDefault, validate, properties.order());
      properties.copyAliases(field::addAlias);
      properties.copyProperties(field);
      fields.add(field);
    }

    /**
     * When parsing JSON, the parser generates a LongNode or IntNode based on the
     * size of the number it encounters. But this may not be expected based on the
     * schema. This method fixes that.
     *
     * @param defaultValue the parsed default value
     * @param fieldType    the field schema
     * @return the default value, now matching the schema
     */
    private JsonNode fixDefaultValue(JsonNode defaultValue, Schema fieldType) {
      if (!(defaultValue instanceof IntNode)) {
        return defaultValue;
      }

      if (fieldType.getType() == Schema.Type.UNION) {
        for (Schema unionedType : fieldType.getTypes()) {
          if (unionedType.getType() == Schema.Type.INT) {
            break;
          } else if (unionedType.getType() == Schema.Type.LONG) {
            return new LongNode(defaultValue.longValue());
          }
        }
        return defaultValue;
      }

      if (fieldType.getType() == Schema.Type.LONG) {
        return new LongNode(defaultValue.longValue());
      }

      return defaultValue;
    }

    /**
     * For "optional schemas" (recognized by the marker property the NullableType
     * production adds), ensure the null schema is in the right place.
     *
     * @param schema       a schema
     * @param defaultValue the intended default value
     * @return the schema, or an optional schema with null in the right place
     */
    private Schema fixOptionalSchema(Schema schema, JsonNode defaultValue) {
      Object optionalType = schema.getObjectProp(OPTIONAL_NULLABLE_TYPE_PROPERTY);
      if (optionalType == null) {
        return schema;
      }

      // The schema is a union schema with 2 types: "null" and a non-"null"
      // schema. The result of this method must not have the property
      // OPTIONAL_NULLABLE_TYPE_PROPERTY.
      Schema nullSchema = schema.getTypes().get(0);
      Schema nonNullSchema = schema.getTypes().get(1);
      boolean nonNullDefault = defaultValue != null && !defaultValue.isNull();

      if (nonNullDefault) {
        return Schema.createUnion(nonNullSchema, nullSchema);
      } else {
        return Schema.createUnion(nullSchema, nonNullSchema);
      }
    }

    @Override
    public void enterMessageDeclaration(MessageDeclarationContext ctx) {
      assert typeStack.isEmpty();
      assert fields.isEmpty();
      assert messageDocComment == null;
      propertiesStack.push(new SchemaProperties(currentNamespace(), false, false, false));
      messageDocComment = getDocComment(ctx);
    }

    @Override
    public void exitMessageDeclaration(MessageDeclarationContext ctx) {
      Schema resultType = typeStack.pop();
      Map<String, JsonNode> properties = propertiesStack.pop().properties;
      String name = identifier(ctx.name);

      Schema request = Schema.createRecord(null, null, null, false, fields);
      fields.clear();

      Protocol.Message message;
      if (ctx.oneway != null) {
        if (resultType.getType() == Schema.Type.NULL) {
          message = protocol.createMessage(name, messageDocComment, properties, request);
        } else {
          throw error("One-way message'" + name + "' must return void", ctx.returnType.start);
        }
      } else {
        List<Schema> errorSchemas = new ArrayList<>();
        errorSchemas.add(Protocol.SYSTEM_ERROR);
        for (IdentifierContext errorContext : ctx.errors) {
          errorSchemas.add(namedSchemaOrUnresolved(fullName(currentNamespace(), identifier(errorContext))));
        }
        message = protocol.createMessage(name, messageDocComment, properties, request, resultType,
            Schema.createUnion(errorSchemas));
      }
      messageDocComment = null;
      protocol.getMessages().put(message.getName(), message);
    }

    @Override
    public void enterFormalParameter(FormalParameterContext ctx) {
      assert typeStack.size() == 1; // The message return type is on the stack; nothing else.
      defaultVariableDocComment = getDocComment(ctx);
    }

    @Override
    public void exitFormalParameter(FormalParameterContext ctx) {
      typeStack.pop();
      defaultVariableDocComment = null;
    }

    @Override
    public void exitResultType(ResultTypeContext ctx) {
      if (typeStack.isEmpty()) {
        // if there's no type, we've parsed 'void': use the null type
        typeStack.push(Schema.create(Schema.Type.NULL));
      }
    }

    @Override
    public void enterFullType(FullTypeContext ctx) {
      propertiesStack.push(new SchemaProperties(currentNamespace(), false, false, false));
    }

    @Override
    public void exitFullType(FullTypeContext ctx) {
      SchemaProperties properties = propertiesStack.pop();

      Schema type = typeStack.element();
      if (type.getObjectProp(OPTIONAL_NULLABLE_TYPE_PROPERTY) != null) {
        // Optional type: put the properties on the non-null content
        properties.copyProperties(type.getTypes().get(1));
      } else {
        properties.copyProperties(type);
      }
    }

    @Override
    public void exitNullableType(NullableTypeContext ctx) {
      Schema type;
      if (ctx.referenceName == null) {
        type = typeStack.pop();
      } else {
        // propertiesStack is empty within resultType->plainType->nullableType, and
        // holds our properties otherwise
        if (propertiesStack.isEmpty() || propertiesStack.peek().hasProperties()) {
          throw error("Type references may not be annotated", ctx.getParent().getStart());
        }
        type = namedSchemaOrUnresolved(fullName(currentNamespace(), identifier(ctx.referenceName)));
      }
      if (ctx.optional != null) {
        type = Schema.createUnion(Schema.create(Schema.Type.NULL), type);
        // Add a marker property to the union (it will be removed when creating fields)
        type.addProp(OPTIONAL_NULLABLE_TYPE_PROPERTY, BooleanNode.TRUE);
      }
      typeStack.push(type);
    }

    @Override
    public void exitPrimitiveType(PrimitiveTypeContext ctx) {
      switch (ctx.typeName.getType()) {
      case IdlParser.Boolean:
        typeStack.push(Schema.create(Schema.Type.BOOLEAN));
        break;
      case IdlParser.Int:
        typeStack.push(Schema.create(Schema.Type.INT));
        break;
      case IdlParser.Long:
        typeStack.push(Schema.create(Schema.Type.LONG));
        break;
      case IdlParser.Float:
        typeStack.push(Schema.create(Schema.Type.FLOAT));
        break;
      case IdlParser.Double:
        typeStack.push(Schema.create(Schema.Type.DOUBLE));
        break;
      case IdlParser.Bytes:
        typeStack.push(Schema.create(Schema.Type.BYTES));
        break;
      case IdlParser.String:
        typeStack.push(Schema.create(Schema.Type.STRING));
        break;
      case IdlParser.Null:
        typeStack.push(Schema.create(Schema.Type.NULL));
        break;
      case IdlParser.Date:
        typeStack.push(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
        break;
      case IdlParser.Time:
        typeStack.push(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));
        break;
      case IdlParser.Timestamp:
        typeStack.push(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
        break;
      case IdlParser.LocalTimestamp:
        typeStack.push(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));
        break;
      case IdlParser.UUID:
        typeStack.push(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)));
        break;
      default: // Only option left: decimal
        int precision = Integer.decode(ctx.precision.getText());
        int scale = ctx.scale == null ? 0 : Integer.decode(ctx.scale.getText());
        typeStack.push(LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES)));
        break;
      }
    }

    @Override
    public void exitArrayType(ArrayTypeContext ctx) {
      typeStack.push(Schema.createArray(typeStack.pop()));
    }

    @Override
    public void exitMapType(MapTypeContext ctx) {
      typeStack.push(Schema.createMap(typeStack.pop()));
    }

    @Override
    public void enterUnionType(UnionTypeContext ctx) {
      // push an empty marker union; we'll replace it with the real union upon exit
      typeStack.push(Schema.createUnion());
    }

    @Override
    public void exitUnionType(UnionTypeContext ctx) {
      List<Schema> types = new ArrayList<>();
      Schema type;
      while ((type = typeStack.pop()).getType() != Schema.Type.UNION) {
        types.add(type);
      }
      Collections.reverse(types); // Popping the stack works in reverse order
      // type is an empty marker union; ignore (drop) it
      typeStack.push(Schema.createUnion(types));
    }

    @Override
    public void exitJsonValue(JsonValueContext ctx) {
      if (ctx.parent instanceof JsonArrayContext) {
        JsonNode value = jsonValues.pop();
        assert jsonValues.peek() instanceof ArrayNode;
        ((ArrayNode) jsonValues.element()).add(value);
      }
    }

    @Override
    public void exitJsonLiteral(JsonLiteralContext ctx) {
      Token literal = ctx.literal;
      switch (literal.getType()) {
      case IdlParser.Null:
        jsonValues.push(NullNode.getInstance());
        break;
      case IdlParser.BTrue:
        jsonValues.push(BooleanNode.TRUE);
        break;
      case IdlParser.BFalse:
        jsonValues.push(BooleanNode.FALSE);
        break;
      case IdlParser.IntegerLiteral:
        String number = literal.getText().replace("_", "");
        char lastChar = number.charAt(number.length() - 1);
        boolean coerceToLong = false;
        if (lastChar == 'l' || lastChar == 'L') {
          coerceToLong = true;
          number = number.substring(0, number.length() - 1);
        }
        long longNumber = Long.decode(number);
        int intNumber = (int) longNumber; // Narrowing cast: if too large a number, the two are different
        jsonValues.push(coerceToLong || intNumber != longNumber ? new LongNode(longNumber) : new IntNode(intNumber));
        break;
      case IdlParser.FloatingPointLiteral:
        jsonValues.push(new DoubleNode(Double.parseDouble(literal.getText())));
        break;
      default: // StringLiteral:
        jsonValues.push(new TextNode(getString(literal)));
        break;
      }
    }

    @Override
    public void enterJsonArray(JsonArrayContext ctx) {
      jsonValues.push(new ArrayNode(null));
    }

    @Override
    public void enterJsonObject(JsonObjectContext ctx) {
      jsonValues.push(new ObjectNode(null));
    }

    @Override
    public void exitJsonPair(JsonPairContext ctx) {
      String name = getString(ctx.name);
      JsonNode value = jsonValues.pop();
      assert jsonValues.peek() instanceof ObjectNode;
      ((ObjectNode) jsonValues.element()).set(name, value);
    }

    private String identifier(IdentifierContext ctx) {
      return ctx.word.getText().replace("`", "");
    }

    private String name(String identifier) {
      int dotPos = identifier.lastIndexOf('.');
      String name = identifier.substring(dotPos + 1);
      return validateName(name, true);
    }

    private String namespace(String identifier, String namespace) {
      int dotPos = identifier.lastIndexOf('.');
      String ns = dotPos < 0 ? namespace : identifier.substring(0, dotPos);
      if (ns == null) {
        return null;
      }
      for (int s = 0, e = ns.indexOf('.'); e > 0; s = e + 1, e = ns.indexOf('.', s)) {
        validateName(ns.substring(s, e), false);
      }
      return ns;
    }

    private String validateName(String name, boolean isTypeName) {
      if (name == null) {
        throw new SchemaParseException("Null name");
      } else if (!VALID_NAME.test(name)) {
        throw new SchemaParseException("Illegal name: " + name);
      }
      if (isTypeName && INVALID_TYPE_NAMES.contains(name)) {
        throw new SchemaParseException("Illegal name: " + name);
      }
      return name;
    }

    private String fullName(String namespace, String typeName) {
      int dotPos = typeName.lastIndexOf('.');
      if (dotPos > -1) {
        return typeName;
      }
      return namespace != null ? namespace + "." + typeName : typeName;
    }

    private String getString(Token stringToken) {
      String stringLiteral = stringToken.getText();
      String betweenQuotes = stringLiteral.substring(1, stringLiteral.length() - 1);
      return StringEscapeUtils.unescapeJava(betweenQuotes);
    }
  }

  private static class SchemaProperties {
    String contextNamespace;
    boolean withNamespace;
    String namespace;
    boolean withAliases;
    List<String> aliases;
    boolean withOrder;
    Schema.Field.Order order;
    Map<String, JsonNode> properties;

    public SchemaProperties(String contextNamespace, boolean withNamespace, boolean withAliases, boolean withOrder) {
      this.contextNamespace = contextNamespace;
      this.withNamespace = withNamespace;
      this.withAliases = withAliases;
      this.aliases = Collections.emptyList();
      this.withOrder = withOrder;
      this.order = Schema.Field.Order.ASCENDING;
      this.properties = new LinkedHashMap<>();
    }

    public void addProperty(String name, JsonNode value, Token firstValueToken) {
      if (withNamespace && "namespace".equals(name)) {
        if (value.isTextual()) {
          namespace = value.textValue();
        } else {
          throw error("@namespace(...) must contain a String value", firstValueToken);
        }
      } else if (withAliases && "aliases".equals(name)) {
        if (value.isArray()) {
          List<String> result = new ArrayList<>();
          Iterator<JsonNode> elements = value.elements();
          elements.forEachRemaining(element -> {
            if (element.isTextual()) {
              result.add(element.textValue());
            } else {
              throw error("@aliases(...) must contain an array of String values", firstValueToken);
            }
          });
          aliases = result;
        } else {
          throw error("@aliases(...) must contain an array of String values", firstValueToken);
        }
      } else if (withOrder && "order".equals(name)) {
        if (value.isTextual()) {
          String orderValue = value.textValue().toUpperCase(Locale.ROOT);
          switch (orderValue) {
          case "ASCENDING":
            order = Schema.Field.Order.ASCENDING;
            break;
          case "DESCENDING":
            order = Schema.Field.Order.DESCENDING;
            break;
          case "IGNORE":
            order = Schema.Field.Order.IGNORE;
            break;
          default:
            throw error("@order(...) must contain \"ASCENDING\", \"DESCENDING\" or \"IGNORE\"", firstValueToken);
          }
        } else {
          throw error("@order(...) must contain a String value", firstValueToken);
        }
      } else {
        properties.put(name, value);
      }
    }

    public String namespace() {
      return namespace == null ? contextNamespace : namespace;
    }

    public Schema.Field.Order order() {
      return order;
    }

    public void copyAliases(Consumer<String> addAlias) {
      aliases.forEach(addAlias);
    }

    public <T extends JsonProperties> T copyProperties(T jsonProperties) {
      properties.forEach(jsonProperties::addProp);
      if (jsonProperties instanceof Schema) {
        Schema schema = (Schema) jsonProperties;
        LogicalType logicalType = LogicalTypes.fromSchemaIgnoreInvalid(schema);
        if (logicalType != null) {
          logicalType.addToSchema(schema);
        }
      }
      return jsonProperties;
    }

    public boolean hasProperties() {
      return !properties.isEmpty();
    }
  }
}
