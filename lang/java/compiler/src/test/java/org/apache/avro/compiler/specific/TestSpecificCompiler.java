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
package org.apache.avro.compiler.specific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.apache.avro.AvroTypeException;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSpecificCompiler {
  private static final Logger LOG = LoggerFactory.getLogger(TestSpecificCompiler.class);

  /**
   * JDK 18+ generates a warning for each member field which does not implement
   * java.io.Serializable. Since Avro is an alternative serialization format, we
   * can just ignore this warning.
   */
  private static final String NON_TRANSIENT_INSTANCE_FIELD_MESSAGE = "non-transient instance field of a serializable class declared with a non-serializable type";

  @TempDir
  public File OUTPUT_DIR;

  private File outputFile;

  @BeforeEach
  public void setUp() {
    this.outputFile = new File(this.OUTPUT_DIR, "SimpleRecord.java");
  }

  private File src = new File("src/test/resources/simple_record.avsc");

  static void assertCompilesWithJavaCompiler(File dstDir, Collection<SpecificCompiler.OutputFile> outputs)
      throws IOException {
    assertCompilesWithJavaCompiler(dstDir, outputs, false);
  }

  /**
   * Uses the system's java compiler to actually compile the generated code.
   */
  static void assertCompilesWithJavaCompiler(File dstDir, Collection<SpecificCompiler.OutputFile> outputs,
      boolean ignoreWarnings) throws IOException {
    if (outputs.isEmpty()) {
      return; // Nothing to compile!
    }

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);

    List<File> javaFiles = new ArrayList<>();
    for (SpecificCompiler.OutputFile o : outputs) {
      javaFiles.add(o.writeToDestination(null, dstDir));
    }

    final List<Diagnostic<?>> warnings = new ArrayList<>();
    DiagnosticListener<JavaFileObject> diagnosticListener = diagnostic -> {
      switch (diagnostic.getKind()) {
      case ERROR:
        // Do not add these to warnings because they will fail the compilation, anyway.
        LOG.error("{}", diagnostic);
        break;
      case WARNING:
      case MANDATORY_WARNING:
        String message = diagnostic.getMessage(Locale.ROOT);
        if (!NON_TRANSIENT_INSTANCE_FIELD_MESSAGE.equals(message)) {
          LOG.warn("{}", diagnostic);
          warnings.add(diagnostic);
        }
        break;
      case NOTE:
      case OTHER:
        LOG.debug("{}", diagnostic);
        break;
      }
    };
    JavaCompiler.CompilationTask cTask = compiler.getTask(null, fileManager, diagnosticListener,
        Collections.singletonList("-Xlint:all"), null, fileManager.getJavaFileObjects(javaFiles.toArray(new File[0])));
    boolean compilesWithoutError = cTask.call();
    assertTrue(compilesWithoutError);
    if (!ignoreWarnings) {
      assertEquals(0, warnings.size(), "Warnings produced when compiling generated code with -Xlint:all");
    }
  }

  private static Schema createSampleRecordSchema(int numStringFields, int numDoubleFields) {
    SchemaBuilder.FieldAssembler<Schema> sb = SchemaBuilder.record("sample.record").fields();
    for (int i = 0; i < numStringFields; i++) {
      sb.name("sf_" + i).type().stringType().noDefault();
    }
    for (int i = 0; i < numDoubleFields; i++) {
      sb.name("df_" + i).type().doubleType().noDefault();
    }
    return sb.endRecord();
  }

  private SpecificCompiler createCompiler() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(this.src);
    SpecificCompiler compiler = new SpecificCompiler(schema);
    String velocityTemplateDir = "src/main/velocity/org/apache/avro/compiler/specific/templates/java/classic/";
    compiler.setTemplateDir(velocityTemplateDir);
    compiler.setStringType(StringType.CharSequence);
    return compiler;
  }

  @Test
  void canReadTemplateFilesOnTheFilesystem() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, OUTPUT_DIR);
    assertTrue(new File(OUTPUT_DIR, "SimpleRecord.java").exists());
  }

  @Test
  void publicFieldVisibility() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PUBLIC);
    assertTrue(compiler.publicFields());
    assertFalse(compiler.privateFields());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // No line, once trimmed, should start with a deprecated field declaration
        // nor a private field declaration. Since the nested builder uses private
        // fields, we cannot do the second check.
        line = line.trim();
        assertFalse(line.startsWith("@Deprecated public int value"),
            "Line started with a deprecated field declaration: " + line);
      }
    }
  }

  @Test
  void createAllArgsConstructor() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    boolean foundAllArgsConstructor = false;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while (!foundAllArgsConstructor && (line = reader.readLine()) != null) {
        foundAllArgsConstructor = line.contains("All-args constructor");
      }
    }
    assertTrue(foundAllArgsConstructor);
  }

  @Test
  void maxValidParameterCounts() throws Exception {
    Schema validSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 0);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR, "testMaxValidParameterCounts1"),
        new SpecificCompiler(validSchema1).compile());

    createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 2, 1);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR, "testMaxValidParameterCounts2"),
        new SpecificCompiler(validSchema1).compile());
  }

  @Test
  void invalidParameterCounts() throws Exception {
    Schema invalidSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT + 1, 0);
    SpecificCompiler compiler = new SpecificCompiler(invalidSchema1);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR, "testInvalidParameterCounts1"), compiler.compile());

    Schema invalidSchema2 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 10);
    compiler = new SpecificCompiler(invalidSchema2);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR, "testInvalidParameterCounts2"), compiler.compile());
  }

  @Test
  void maxParameterCounts() throws Exception {
    Schema validSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 0);
    assertTrue(new SpecificCompiler(validSchema1).compile().size() > 0);

    Schema validSchema2 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 2, 1);
    assertTrue(new SpecificCompiler(validSchema2).compile().size() > 0);

    Schema validSchema3 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 1, 1);
    assertTrue(new SpecificCompiler(validSchema3).compile().size() > 0);

    Schema validSchema4 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT + 1, 0);
    assertTrue(new SpecificCompiler(validSchema4).compile().size() > 0);
  }

  @Test
  void calcAllArgConstructorParameterUnitsFailure() {
    assertThrows(RuntimeException.class, () -> {
      Schema nonRecordSchema = SchemaBuilder.array().items().booleanType();
      new SpecificCompiler().calcAllArgConstructorParameterUnits(nonRecordSchema);
    });
  }

  @Test
  void privateFieldVisibility() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PRIVATE);
    assertFalse(compiler.publicFields());
    assertTrue(compiler.privateFields());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        // No line, once trimmed, should start with a public field declaration
        // or with a deprecated public field declaration
        line = line.trim();
        assertFalse(line.startsWith("public int value"), "Line started with a public field declaration: " + line);
        assertFalse(line.startsWith("@Deprecated public int value"),
            "Line started with a deprecated field declaration: " + line);
      }
    }
  }

  @Test
  void settersCreatedByDefault() throws IOException {
    SpecificCompiler compiler = createCompiler();
    assertTrue(compiler.isCreateSetters());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    int foundSetters = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // We should find the setter in the main class
        line = line.trim();
        if (line.startsWith("public void setValue(")) {
          foundSetters++;
        }
      }
    }
    assertEquals(1, foundSetters, "Found the wrong number of setters");
  }

  @Test
  void settersNotCreatedWhenOptionTurnedOff() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setCreateSetters(false);
    assertFalse(compiler.isCreateSetters());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // No setter should be found
        line = line.trim();
        assertFalse(line.startsWith("public void setValue("), "No line should include the setter: " + line);
      }
    }
  }

  @Test
  void settingOutputCharacterEncoding() throws Exception {
    SpecificCompiler compiler = createCompiler();
    // Generated file in default encoding
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    byte[] fileInDefaultEncoding = new byte[(int) this.outputFile.length()];
    FileInputStream is = new FileInputStream(this.outputFile);
    is.read(fileInDefaultEncoding);
    is.close(); // close input stream otherwise delete might fail
    if (!this.outputFile.delete()) {
      throw new IllegalStateException("unable to delete " + this.outputFile); // delete otherwise compiler might not
      // overwrite because src timestamp hasn't
      // changed.
    }
    // Generate file in another encoding (make sure it has different number of bytes
    // per character)
    String differentEncoding = Charset.defaultCharset().equals(Charset.forName("UTF-16")) ? "UTF-32" : "UTF-16";
    compiler.setOutputCharacterEncoding(differentEncoding);
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    byte[] fileInDifferentEncoding = new byte[(int) this.outputFile.length()];
    is = new FileInputStream(this.outputFile);
    is.read(fileInDifferentEncoding);
    is.close();
    // Compare as bytes
    assertThat("Generated file should contain different bytes after setting non-default encoding",
        fileInDefaultEncoding, not(equalTo(fileInDifferentEncoding)));
    // Compare as strings
    assertThat("Generated files should contain the same characters in the proper encodings",
        new String(fileInDefaultEncoding), equalTo(new String(fileInDifferentEncoding, differentEncoding)));
  }

  @Test
  void javaTypeWithDecimalLogicalTypeEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema localTimestampSchema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    // Date/time types should always use upper level java classes
    // Decimal type target class depends on configuration
    // UUID should always be CharSequence since we haven't added its
    // support in SpecificRecord
    assertEquals("java.time.LocalDate", compiler.javaType(dateSchema), "Should use LocalDate for date type");
    assertEquals("java.time.LocalTime", compiler.javaType(timeSchema), "Should use LocalTime for time-millis type");
    assertEquals("java.time.Instant", compiler.javaType(timestampSchema),
        "Should use DateTime for timestamp-millis type");
    assertEquals("java.time.LocalDateTime", compiler.javaType(localTimestampSchema),
        "Should use LocalDateTime for local-timestamp-millis type");
    assertEquals("java.math.BigDecimal", compiler.javaType(decimalSchema), "Should use Java BigDecimal type");
    assertEquals("new org.apache.avro.Conversions.UUIDConversion()", compiler.conversionInstance(uuidSchema),
        "Should use org.apache.avro.Conversions.UUIDConversion() type");
  }

  @Test
  void javaTypeWithDecimalLogicalTypeDisabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    // Date/time types should always use upper level java classes
    // Decimal type target class depends on configuration
    // UUID should always be CharSequence since we haven't added its
    // support in SpecificRecord
    assertEquals("java.time.LocalDate", compiler.javaType(dateSchema), "Should use LocalDate for date type");
    assertEquals("java.time.LocalTime", compiler.javaType(timeSchema), "Should use LocalTime for time-millis type");
    assertEquals("java.time.Instant", compiler.javaType(timestampSchema),
        "Should use DateTime for timestamp-millis type");
    assertEquals("java.nio.ByteBuffer", compiler.javaType(decimalSchema), "Should use ByteBuffer type");
    assertEquals("new org.apache.avro.Conversions.UUIDConversion()", compiler.conversionInstance(uuidSchema),
        "Should use org.apache.avro.Conversions.UUIDConversion() type");
  }

  @Test
  void javaTypeWithDateTimeTypes() throws Exception {
    SpecificCompiler compiler = createCompiler();

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMicrosSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMicrosSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampNanosSchema = LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));

    // Date/time types should always use upper level java classes
    assertEquals("java.time.LocalDate", compiler.javaType(dateSchema), "Should use java.time.LocalDate for date type");
    assertEquals("java.time.LocalTime", compiler.javaType(timeSchema),
        "Should use java.time.LocalTime for time-millis type");
    assertEquals("java.time.Instant", compiler.javaType(timestampSchema),
        "Should use java.time.Instant for timestamp-millis type");
    assertEquals("java.time.LocalTime", compiler.javaType(timeMicrosSchema),
        "Should use java.time.LocalTime for time-micros type");
    assertEquals("java.time.Instant", compiler.javaType(timestampMicrosSchema),
        "Should use java.time.Instant for timestamp-micros type");
    assertEquals("java.time.Instant", compiler.javaType(timestampNanosSchema),
        "Should use java.time.Instant for timestamp-nanos type");
  }

  @Test
  void javaUnbox() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    Schema boolSchema = Schema.create(Schema.Type.BOOLEAN);
    assertEquals("int", compiler.javaUnbox(intSchema, false), "Should use int for Type.INT");
    assertEquals("long", compiler.javaUnbox(longSchema, false), "Should use long for Type.LONG");
    assertEquals("float", compiler.javaUnbox(floatSchema, false), "Should use float for Type.FLOAT");
    assertEquals("double", compiler.javaUnbox(doubleSchema, false), "Should use double for Type.DOUBLE");
    assertEquals("boolean", compiler.javaUnbox(boolSchema, false), "Should use boolean for Type.BOOLEAN");

    // see AVRO-2569
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    assertEquals("void", compiler.javaUnbox(nullSchema, true), "Should use void for Type.NULL");

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    // Date/time types should always use upper level java classes, even though
    // their underlying representations are primitive types
    assertEquals("java.time.LocalDate", compiler.javaUnbox(dateSchema, false), "Should use LocalDate for date type");
    assertEquals("java.time.LocalTime", compiler.javaUnbox(timeSchema, false),
        "Should use LocalTime for time-millis type");
    assertEquals("java.time.Instant", compiler.javaUnbox(timestampSchema, false),
        "Should use DateTime for timestamp-millis type");
  }

  @Test
  void javaUnboxDateTime() throws Exception {
    SpecificCompiler compiler = createCompiler();

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    // Date/time types should always use upper level java classes, even though
    // their underlying representations are primitive types
    assertEquals("java.time.LocalDate", compiler.javaUnbox(dateSchema, false),
        "Should use java.time.LocalDate for date type");
    assertEquals("java.time.LocalTime", compiler.javaUnbox(timeSchema, false),
        "Should use java.time.LocalTime for time-millis type");
    assertEquals("java.time.Instant", compiler.javaUnbox(timestampSchema, false),
        "Should use java.time.Instant for timestamp-millis type");
  }

  @Test
  void nullableLogicalTypesJavaUnboxDecimalTypesEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    // Nullable types should return boxed types instead of primitive types
    Schema nullableDecimalSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)));
    Schema nullableDecimalSchema2 = Schema.createUnion(
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)), Schema.create(Schema.Type.NULL));
    assertEquals(compiler.javaUnbox(nullableDecimalSchema1, false), "java.math.BigDecimal", "Should return boxed type");
    assertEquals(compiler.javaUnbox(nullableDecimalSchema2, false), "java.math.BigDecimal", "Should return boxed type");
  }

  @Test
  void nullableLogicalTypesJavaUnboxDecimalTypesDisabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    // Since logical decimal types are disabled, a ByteBuffer is expected.
    Schema nullableDecimalSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)));
    Schema nullableDecimalSchema2 = Schema.createUnion(
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)), Schema.create(Schema.Type.NULL));
    assertEquals(compiler.javaUnbox(nullableDecimalSchema1, false), "java.nio.ByteBuffer", "Should return boxed type");
    assertEquals(compiler.javaUnbox(nullableDecimalSchema2, false), "java.nio.ByteBuffer", "Should return boxed type");
  }

  @Test
  void nullableTypesJavaUnbox() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    // Nullable types should return boxed types instead of primitive types
    Schema nullableIntSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    Schema nullableIntSchema2 = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL));
    assertEquals(compiler.javaUnbox(nullableIntSchema1, false), "java.lang.Integer", "Should return boxed type");
    assertEquals(compiler.javaUnbox(nullableIntSchema2, false), "java.lang.Integer", "Should return boxed type");

    Schema nullableLongSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    Schema nullableLongSchema2 = Schema.createUnion(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL));
    assertEquals(compiler.javaUnbox(nullableLongSchema1, false), "java.lang.Long", "Should return boxed type");
    assertEquals(compiler.javaUnbox(nullableLongSchema2, false), "java.lang.Long", "Should return boxed type");

    Schema nullableFloatSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT));
    Schema nullableFloatSchema2 = Schema.createUnion(Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.NULL));
    assertEquals(compiler.javaUnbox(nullableFloatSchema1, false), "java.lang.Float", "Should return boxed type");
    assertEquals(compiler.javaUnbox(nullableFloatSchema2, false), "java.lang.Float", "Should return boxed type");

    Schema nullableDoubleSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.DOUBLE));
    Schema nullableDoubleSchema2 = Schema.createUnion(Schema.create(Schema.Type.DOUBLE),
        Schema.create(Schema.Type.NULL));
    assertEquals(compiler.javaUnbox(nullableDoubleSchema1, false), "java.lang.Double", "Should return boxed type");
    assertEquals(compiler.javaUnbox(nullableDoubleSchema2, false), "java.lang.Double", "Should return boxed type");

    Schema nullableBooleanSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN));
    Schema nullableBooleanSchema2 = Schema.createUnion(Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.NULL));
    assertEquals(compiler.javaUnbox(nullableBooleanSchema1, false), "java.lang.Boolean", "Should return boxed type");
    assertEquals(compiler.javaUnbox(nullableBooleanSchema2, false), "java.lang.Boolean", "Should return boxed type");
  }

  @Test
  void getUsedCustomLogicalTypeFactories() throws Exception {
    LogicalTypes.register("string-custom", new StringCustomLogicalTypeFactory());

    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    final Schema schema = new Schema.Parser().parse("{\"type\":\"record\"," + "\"name\":\"NestedLogicalTypesRecord\","
        + "\"namespace\":\"org.apache.avro.codegentest.testdata\","
        + "\"doc\":\"Test nested types with logical types in generated Java classes\"," + "\"fields\":["
        + "{\"name\":\"nestedRecord\",\"type\":" + "{\"type\":\"record\",\"name\":\"NestedRecord\",\"fields\":"
        + "[{\"name\":\"nullableDateField\"," + "\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}},"
        + "{\"name\":\"myLogical\",\"type\":{\"type\":\"string\",\"logicalType\":\"string-custom\"}}]}");

    final Map<String, String> usedCustomLogicalTypeFactories = compiler.getUsedCustomLogicalTypeFactories(schema);
    assertEquals(1, usedCustomLogicalTypeFactories.size());
    final Map.Entry<String, String> entry = usedCustomLogicalTypeFactories.entrySet().iterator().next();
    assertEquals("string-custom", entry.getKey());
    assertEquals("org.apache.avro.compiler.specific.TestSpecificCompiler.StringCustomLogicalTypeFactory",
        entry.getValue());
  }

  @Test
  void emptyGetUsedCustomLogicalTypeFactories() throws Exception {
    LogicalTypes.register("string-custom", new StringCustomLogicalTypeFactory());

    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    final Schema schema = new Schema.Parser().parse("{\"type\":\"record\"," + "\"name\":\"NestedLogicalTypesRecord\","
        + "\"namespace\":\"org.apache.avro.codegentest.testdata\","
        + "\"doc\":\"Test nested types with logical types in generated Java classes\"," + "\"fields\":["
        + "{\"name\":\"nestedRecord\"," + "\"type\":{\"type\":\"record\",\"name\":\"NestedRecord\",\"fields\":"
        + "[{\"name\":\"nullableDateField\","
        + "\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}}]}");

    final Map<String, String> usedCustomLogicalTypeFactories = compiler.getUsedCustomLogicalTypeFactories(schema);
    assertEquals(0, usedCustomLogicalTypeFactories.size());
  }

  @Test
  void getUsedConversionClassesForNullableLogicalTypes() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema nullableDecimal1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)));
    Schema schemaWithNullableDecimal1 = Schema.createRecord("WithNullableDecimal", "", "", false,
        Collections.singletonList(new Schema.Field("decimal", nullableDecimal1, "", null)));

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schemaWithNullableDecimal1);
    assertEquals(1, usedConversionClasses.size());
    assertEquals("org.apache.avro.Conversions.DecimalConversion", usedConversionClasses.iterator().next());
  }

  @Test
  void getUsedConversionClassesForNullableTimestamps() throws Exception {
    SpecificCompiler compiler = createCompiler();

    // timestamp-millis and timestamp-micros used to cause collisions when both were
    // present or added as converters (AVRO-2481).
    final Schema tsMillis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    final Schema tsMicros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    final Schema tsNanos = LogicalTypes.timestampNanos().addToSchema(Schema.create(Schema.Type.LONG));

    final Collection<String> conversions = compiler.getUsedConversionClasses(SchemaBuilder.record("WithTimestamps")
        .fields().name("tsMillis").type(tsMillis).noDefault().name("tsMillisOpt").type().unionOf().nullType().and()
        .type(tsMillis).endUnion().noDefault().name("tsMicros").type(tsMicros).noDefault().name("tsMicrosOpt").type()
        .unionOf().nullType().and().type(tsMicros).endUnion().noDefault().name("tsNanos").type(tsNanos).noDefault()
        .name("tsNanosOpt").type().unionOf().nullType().and().type(tsNanos).endUnion().noDefault().endRecord());

    assertEquals(3, conversions.size());
    assertThat(conversions, hasItem("org.apache.avro.data.TimeConversions.TimestampMillisConversion"));
    assertThat(conversions, hasItem("org.apache.avro.data.TimeConversions.TimestampMicrosConversion"));
    assertThat(conversions, hasItem("org.apache.avro.data.TimeConversions.TimestampNanosConversion"));
  }

  @Test
  void getUsedConversionClassesForNullableLogicalTypesInNestedRecord() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesRecord\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"nestedRecord\",\"type\":{\"type\":\"record\",\"name\":\"NestedRecord\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    assertEquals(1, usedConversionClasses.size());
    assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  void getUsedConversionClassesForNullableLogicalTypesInArray() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NullableLogicalTypesArray\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"arrayOfLogicalType\",\"type\":{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    assertEquals(1, usedConversionClasses.size());
    assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  void getUsedConversionClassesForNullableLogicalTypesInArrayOfRecords() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesArray\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"arrayOfRecords\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"RecordInArray\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    assertEquals(1, usedConversionClasses.size());
    assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  void getUsedConversionClassesForNullableLogicalTypesInUnionOfRecords() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesUnion\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"unionOfRecords\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"RecordInUnion\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}]}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    assertEquals(1, usedConversionClasses.size());
    assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  void getUsedConversionClassesForNullableLogicalTypesInMapOfRecords() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesMap\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"mapOfRecords\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"RecordInMap\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]},\"avro.java.string\":\"String\"}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    assertEquals(1, usedConversionClasses.size());
    assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  /**
   * Checks that identifiers that may cause problems in Java code will compile
   * correctly when used in a generated specific record.
   *
   * @param schema                         A schema with an identifier __test__
   *                                       that will be replaced.
   * @param throwsTypeExceptionOnPrimitive If true, using a reserved word that is
   *                                       also an Avro primitive type name must
   *                                       throw an exception instead of
   *                                       generating code.
   * @param dstDirPrefix                   Where to generate the java code before
   *                                       compiling.
   */
  public void testManglingReservedIdentifiers(String schema, boolean throwsTypeExceptionOnPrimitive,
      String dstDirPrefix) throws IOException {
    Set<String> reservedIdentifiers = new HashSet<>();
    reservedIdentifiers.addAll(SpecificData.RESERVED_WORDS);
    reservedIdentifiers.addAll(SpecificData.TYPE_IDENTIFIER_RESERVED_WORDS);
    reservedIdentifiers.addAll(SpecificData.ACCESSOR_MUTATOR_RESERVED_WORDS);
    reservedIdentifiers.addAll(SpecificData.ERROR_RESERVED_WORDS);
    for (String reserved : reservedIdentifiers) {
      try {
        Schema s = new Schema.Parser().parse(schema.replace("__test__", reserved));
        assertCompilesWithJavaCompiler(new File(OUTPUT_DIR, dstDirPrefix + "_" + reserved),
            new SpecificCompiler(s).compile());
      } catch (AvroTypeException e) {
        if (!(throwsTypeExceptionOnPrimitive && e.getMessage().contains("Schemas may not be named after primitives")))
          throw e;
      }
    }
  }

  @Test
  void mangleRecordName() throws Exception {
    testManglingReservedIdentifiers(
        SchemaBuilder.record("__test__").fields().requiredInt("field").endRecord().toString(), true,
        "testMangleRecordName");
  }

  @Test
  void mangleRecordNamespace() throws Exception {
    testManglingReservedIdentifiers(
        SchemaBuilder.record("__test__.Record").fields().requiredInt("field").endRecord().toString(), false,
        "testMangleRecordNamespace");
  }

  @Test
  void mangleField() throws Exception {
    testManglingReservedIdentifiers(
        SchemaBuilder.record("Record").fields().requiredInt("__test__").endRecord().toString(), false,
        "testMangleField");
  }

  @Test
  void mangleEnumName() throws Exception {
    testManglingReservedIdentifiers(SchemaBuilder.enumeration("__test__").symbols("reserved").toString(), true,
        "testMangleEnumName");
  }

  @Test
  void mangleEnumSymbol() throws Exception {
    testManglingReservedIdentifiers(SchemaBuilder.enumeration("Enum").symbols("__test__").toString(), false,
        "testMangleEnumSymbol");
  }

  @Test
  void mangleFixedName() throws Exception {
    testManglingReservedIdentifiers(SchemaBuilder.fixed("__test__").size(2).toString(), true, "testMangleFixedName");
  }

  @Test
  void logicalTypesWithMultipleFields() throws Exception {
    Schema logicalTypesWithMultipleFields = new Schema.Parser()
        .parse(new File("src/test/resources/logical_types_with_multiple_fields.avsc"));
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR, "testLogicalTypesWithMultipleFields"),
        new SpecificCompiler(logicalTypesWithMultipleFields).compile(), true);
  }

  @Test
  void unionAndFixedFields() throws Exception {
    Schema unionTypesWithMultipleFields = new Schema.Parser()
        .parse(new File("src/test/resources/union_and_fixed_fields.avsc"));
    assertCompilesWithJavaCompiler(new File(this.outputFile, "testUnionAndFixedFields"),
        new SpecificCompiler(unionTypesWithMultipleFields).compile());
  }

  @Test
  void logicalTypesWithMultipleFieldsDateTime() throws Exception {
    Schema logicalTypesWithMultipleFields = new Schema.Parser()
        .parse(new File("src/test/resources/logical_types_with_multiple_fields.avsc"));
    assertCompilesWithJavaCompiler(new File(this.outputFile, "testLogicalTypesWithMultipleFieldsDateTime"),
        new SpecificCompiler(logicalTypesWithMultipleFields).compile());
  }

  @Test
  void conversionInstanceWithDecimalLogicalTypeDisabled() throws Exception {
    final SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    final Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    final Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    final Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    final Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    final Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    assertEquals("new org.apache.avro.data.TimeConversions.DateConversion()", compiler.conversionInstance(dateSchema),
        "Should use date conversion for date type");
    assertEquals("new org.apache.avro.data.TimeConversions.TimeMillisConversion()",
        compiler.conversionInstance(timeSchema), "Should use time conversion for time type");
    assertEquals("new org.apache.avro.data.TimeConversions.TimestampMillisConversion()",
        compiler.conversionInstance(timestampSchema), "Should use timestamp conversion for date type");
    assertEquals("null", compiler.conversionInstance(decimalSchema), "Should use null for decimal if the flag is off");
    assertEquals("new org.apache.avro.Conversions.UUIDConversion()", compiler.conversionInstance(uuidSchema),
        "Should use org.apache.avro.Conversions.UUIDConversion() for uuid if the flag is off");
  }

  @Test
  void conversionInstanceWithDecimalLogicalTypeEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    assertEquals("new org.apache.avro.data.TimeConversions.DateConversion()", compiler.conversionInstance(dateSchema),
        "Should use date conversion for date type");
    assertEquals("new org.apache.avro.data.TimeConversions.TimeMillisConversion()",
        compiler.conversionInstance(timeSchema), "Should use time conversion for time type");
    assertEquals("new org.apache.avro.data.TimeConversions.TimestampMillisConversion()",
        compiler.conversionInstance(timestampSchema), "Should use timestamp conversion for date type");
    assertEquals("new org.apache.avro.Conversions.DecimalConversion()", compiler.conversionInstance(decimalSchema),
        "Should use null for decimal if the flag is off");
    assertEquals("new org.apache.avro.Conversions.UUIDConversion()", compiler.conversionInstance(uuidSchema),
        "Should use org.apache.avro.Conversions.UUIDConversion() for uuid if the flag is off");
  }

  @Test
  void pojoWithOptionalTurnedOffByDefault() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        assertFalse(line.contains("Optional"));
      }
    }
  }

  @Test
  void pojoWithOptionalCreatedWhenOptionTurnedOn() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setGettersReturnOptional(true);
    // compiler.setCreateOptionalGetters(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    int optionalFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("Optional")) {
          optionalFound++;
        }
      }
    }
    assertEquals(9, optionalFound);
  }

  @Test
  void pojoWithOptionalCreateForNullableFieldsWhenOptionTurnedOn() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setGettersReturnOptional(true);
    compiler.setOptionalGettersForNullableFieldsOnly(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    int optionalFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("Optional")) {
          optionalFound++;
        }
      }
    }
    assertEquals(5, optionalFound);
  }

  @Test
  void pojoWithOptionalCreatedWhenOptionalForEverythingTurnedOn() throws IOException {
    SpecificCompiler compiler = createCompiler();
    // compiler.setGettersReturnOptional(true);
    compiler.setCreateOptionalGetters(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    int optionalFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("Optional")) {
          optionalFound++;
        }
      }
    }
    assertEquals(17, optionalFound);
  }

  @Test
  void pojoWithOptionalOnlyWhenNullableCreatedTurnedOnAndGettersReturnOptionalTurnedOff() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setOptionalGettersForNullableFieldsOnly(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        // no optionals since gettersReturnOptionalOnlyForNullable is false
        assertFalse(line.contains("Optional"));
      }
    }
  }

  @Test
  void additionalToolsAreInjectedIntoTemplate() throws Exception {
    SpecificCompiler compiler = createCompiler();
    List<Object> customTools = new ArrayList<>();
    customTools.add(new String());
    compiler.setAdditionalVelocityTools(customTools);
    compiler.setTemplateDir("src/test/resources/templates_with_custom_tools/");
    compiler.compileToDestination(this.src, this.OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    int itWorksFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("It works!")) {
          itWorksFound++;
        }
      }
    }
    assertEquals(1, itWorksFound);
  }

  @Test
  void pojoWithUUID() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setOptionalGettersForNullableFieldsOnly(true);
    File avsc = new File("src/main/resources/logical-uuid.avsc");
    compiler.compileToDestination(avsc, OUTPUT_DIR);
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("guid")) {
          assertTrue(line.contains("java.util.UUID"));
        }
      }
    }
  }

  public static class StringCustomLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
    @Override
    public LogicalType fromSchema(Schema schema) {
      return new LogicalType("string-custom");
    }
  }

  @Test
  void fieldWithUnderscore_avro3826() {
    String jsonSchema = "{\n" + "  \"name\": \"Value\",\n" + "  \"type\": \"record\",\n" + "  \"fields\": [\n"
        + "    { \"name\": \"__deleted\",  \"type\": \"string\"\n" + "    }\n" + "  ]\n" + "}";
    Collection<SpecificCompiler.OutputFile> outputs = new SpecificCompiler(new Schema.Parser().parse(jsonSchema))
        .compile();
    assertEquals(1, outputs.size());
    SpecificCompiler.OutputFile outputFile = outputs.iterator().next();
    assertTrue(outputFile.contents.contains("getDeleted()"));
    assertFalse(outputFile.contents.contains("$0"));
    assertFalse(outputFile.contents.contains("$1"));

    String jsonSchema2 = "{\n" + "  \"name\": \"Value\",  \"type\": \"record\",\n" + "  \"fields\": [\n"
        + "    { \"name\": \"__deleted\",  \"type\": \"string\"},\n"
        + "    { \"name\": \"_deleted\",  \"type\": \"string\"}\n" + "  ]\n" + "}";
    Collection<SpecificCompiler.OutputFile> outputs2 = new SpecificCompiler(new Schema.Parser().parse(jsonSchema2))
        .compile();
    assertEquals(1, outputs2.size());
    SpecificCompiler.OutputFile outputFile2 = outputs2.iterator().next();

    assertTrue(outputFile2.contents.contains("getDeleted()"));
    assertTrue(outputFile2.contents.contains("getDeleted$0()"));
    assertFalse(outputFile.contents.contains("$1"));

    String jsonSchema3 = "{\n" + "  \"name\": \"Value\",  \"type\": \"record\",\n" + "  \"fields\": [\n"
        + "    { \"name\": \"__deleted\",  \"type\": \"string\"},\n"
        + "    { \"name\": \"_deleted\",  \"type\": \"string\"},\n"
        + "    { \"name\": \"deleted\",  \"type\": \"string\"}\n" + "  ]\n" + "}";
    Collection<SpecificCompiler.OutputFile> outputs3 = new SpecificCompiler(new Schema.Parser().parse(jsonSchema3))
        .compile();
    assertEquals(1, outputs3.size());
    SpecificCompiler.OutputFile outputFile3 = outputs3.iterator().next();

    assertTrue(outputFile3.contents.contains("getDeleted()"));
    assertTrue(outputFile3.contents.contains("getDeleted$0()"));
    assertTrue(outputFile3.contents.contains("getDeleted$1()"));
    assertFalse(outputFile3.contents.contains("$2"));

    String jsonSchema4 = "{\n" + "  \"name\": \"Value\",  \"type\": \"record\",\n" + "  \"fields\": [\n"
        + "    { \"name\": \"__deleted\",  \"type\": \"string\"},\n"
        + "    { \"name\": \"_deleted\",  \"type\": \"string\"},\n"
        + "    { \"name\": \"deleted\",  \"type\": \"string\"},\n"
        + "    { \"name\": \"Deleted\",  \"type\": \"string\"}\n" + "  ]\n" + "}";
    Collection<SpecificCompiler.OutputFile> outputs4 = new SpecificCompiler(new Schema.Parser().parse(jsonSchema4))
        .compile();
    assertEquals(1, outputs4.size());
    SpecificCompiler.OutputFile outputFile4 = outputs4.iterator().next();

    assertTrue(outputFile4.contents.contains("getDeleted()"));
    assertTrue(outputFile4.contents.contains("getDeleted$0()"));
    assertTrue(outputFile4.contents.contains("getDeleted$1()"));
    assertTrue(outputFile4.contents.contains("getDeleted$2()"));
    assertFalse(outputFile4.contents.contains("$3"));
  }

}
