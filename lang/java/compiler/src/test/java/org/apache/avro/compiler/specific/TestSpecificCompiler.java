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
package org.apache.avro.compiler.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.StringType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

@RunWith(JUnit4.class)
public class TestSpecificCompiler {
  private static final Logger LOG = LoggerFactory.getLogger(TestSpecificCompiler.class);

  private final String schemaSrcPath = "src/test/resources/simple_record.avsc";
  private final String velocityTemplateDir =
      "src/main/velocity/org/apache/avro/compiler/specific/templates/java/classic/";
  private File src;
  private File outputDir;
  private File outputFile;

  @Before
  public void setUp() {
    this.src = new File(this.schemaSrcPath);
    this.outputDir = AvroTestUtil.tempDirectory(getClass(), "specific-output");
    this.outputFile = new File(this.outputDir, "SimpleRecord.java");
    if (outputFile.exists() && !outputFile.delete()) {
      throw new IllegalStateException("unable to delete " + outputFile);
    }
  }

  @After
  public void tearDow() {
    if (this.outputFile != null) {
      this.outputFile.delete();
    }
  }

  /** Uses the system's java compiler to actually compile the generated code. */
  static void assertCompilesWithJavaCompiler(Collection<SpecificCompiler.OutputFile> outputs)
          throws IOException {
    if (outputs.isEmpty())
      return;               // Nothing to compile!

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager =
            compiler.getStandardFileManager(null, null, null);

    File dstDir = AvroTestUtil.tempFile(TestSpecificCompiler.class, "realCompiler");
    List<File> javaFiles = new ArrayList<File>();
    for (SpecificCompiler.OutputFile o : outputs) {
      javaFiles.add(o.writeToDestination(null, dstDir));
    }

    final List<Diagnostic<?>> warnings = new ArrayList<Diagnostic<?>>();
    DiagnosticListener<JavaFileObject> diagnosticListener = new DiagnosticListener<JavaFileObject>() {
      @Override
      public void report(Diagnostic<? extends JavaFileObject> diagnostic) {
        switch (diagnostic.getKind()) {
        case ERROR:
          // Do not add these to warnings becuase they will fail the compile, anyway.
          LOG.error("{}", diagnostic);
          break;
        case WARNING:
        case MANDATORY_WARNING:
          LOG.warn("{}", diagnostic);
          warnings.add(diagnostic);
          break;
        case NOTE:
        case OTHER:
          LOG.debug("{}", diagnostic);
          break;
        }
      }
    };
    JavaCompiler.CompilationTask cTask = compiler.getTask(null, fileManager,
            diagnosticListener, Collections.singletonList("-Xlint:all"), null,
            fileManager.getJavaFileObjects(javaFiles.toArray(new File[javaFiles.size()])));
    boolean compilesWithoutError = cTask.call();
    assertTrue(compilesWithoutError);
    assertEquals("Warnings produced when compiling generated code with -Xlint:all", 0, warnings.size());
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
    compiler.setTemplateDir(this.velocityTemplateDir);
    compiler.setStringType(StringType.CharSequence);
    return compiler;
  }

  @Test
  public void testCanReadTemplateFilesOnTheFilesystem() throws IOException, URISyntaxException{
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
  }

  @Test
  public void testPublicFieldVisibility() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PUBLIC);
    assertFalse(compiler.deprecatedFields());
    assertTrue(compiler.publicFields());
    assertFalse(compiler.privateFields());
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
    BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
    String line = null;
    while ((line = reader.readLine()) != null) {
      // No line, once trimmed, should start with a deprecated field declaration
      // nor a private field declaration.  Since the nested builder uses private
      // fields, we cannot do the second check.
      line = line.trim();
      assertFalse("Line started with a deprecated field declaration: " + line,
        line.startsWith("@Deprecated public int value"));
    }
    reader.close();
  }

  @Test
  public void testCreateAllArgsConstructor() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
    BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
    String line = null;
    boolean foundAllArgsConstructor = false;
    while (!foundAllArgsConstructor && (line = reader.readLine()) != null) {
      foundAllArgsConstructor = line.contains("All-args constructor");
    }
    reader.close();
    assertTrue(foundAllArgsConstructor);
  }

  @Test
  public void testMaxValidParameterCounts() throws Exception {
    Schema validSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 0);
    assertCompilesWithJavaCompiler(new SpecificCompiler(validSchema1).compile());

    Schema validSchema2 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 2, 1);
    assertCompilesWithJavaCompiler(new SpecificCompiler(validSchema1).compile());
  }

  @Test
  public void testInvalidParameterCounts() throws Exception {
    Schema invalidSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT + 1, 0);
    SpecificCompiler compiler = new SpecificCompiler(invalidSchema1);
    assertCompilesWithJavaCompiler(compiler.compile());

    Schema invalidSchema2 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 10);
    compiler = new SpecificCompiler(invalidSchema2);
    assertCompilesWithJavaCompiler(compiler.compile());
  }

  @Test
  public void testMaxParameterCounts() throws Exception {
    Schema validSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 0);
    assertTrue(new SpecificCompiler(validSchema1).compile().size() > 0);

    Schema validSchema2 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 2, 1);
    assertTrue(new SpecificCompiler(validSchema2).compile().size() > 0);

    Schema validSchema3 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 1, 1);
    assertTrue(new SpecificCompiler(validSchema3).compile().size() > 0);

    Schema validSchema4 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT + 1, 0);
    assertTrue(new SpecificCompiler(validSchema4).compile().size() > 0);
  }

  @Test(expected=RuntimeException.class)
  public void testCalcAllArgConstructorParameterUnitsFailure() {
    Schema nonRecordSchema = SchemaBuilder.array().items().booleanType();
    new SpecificCompiler().calcAllArgConstructorParameterUnits(nonRecordSchema);
  }

  @Test
  public void testPublicDeprecatedFieldVisibility() throws IOException {
    SpecificCompiler compiler = createCompiler();
    assertTrue(compiler.deprecatedFields());
    assertTrue(compiler.publicFields());
    assertFalse(compiler.privateFields());
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
    BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
    String line = null;
    while ((line = reader.readLine()) != null) {
      // No line, once trimmed, should start with a public field declaration
      line = line.trim();
      assertFalse("Line started with a public field declaration: " + line,
        line.startsWith("public int value"));
    }
    reader.close();
  }

  @Test
  public void testPrivateFieldVisibility() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PRIVATE);
    assertFalse(compiler.deprecatedFields());
    assertFalse(compiler.publicFields());
    assertTrue(compiler.privateFields());
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
    BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
    String line = null;
    while ((line = reader.readLine()) != null) {
      // No line, once trimmed, should start with a public field declaration
      // or with a deprecated public field declaration
      line = line.trim();
      assertFalse("Line started with a public field declaration: " + line,
        line.startsWith("public int value"));
      assertFalse("Line started with a deprecated field declaration: " + line,
        line.startsWith("@Deprecated public int value"));
    }
    reader.close();
  }

  @Test
  public void testSettersCreatedByDefault() throws IOException {
    SpecificCompiler compiler = createCompiler();
    assertTrue(compiler.isCreateSetters());
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
    BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
    int foundSetters = 0;
    String line = null;
    while ((line = reader.readLine()) != null) {
      // We should find the setter in the main class
      line = line.trim();
      if (line.startsWith("public void setValue(")) {
        foundSetters++;
      }
    }
    reader.close();
    assertEquals("Found the wrong number of setters", 1, foundSetters);
  }

  @Test
  public void testSettersNotCreatedWhenOptionTurnedOff() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setCreateSetters(false);
    assertFalse(compiler.isCreateSetters());
    compiler.compileToDestination(this.src, this.outputDir);
    assertTrue(this.outputFile.exists());
    BufferedReader reader = new BufferedReader(new FileReader(this.outputFile));
    String line = null;
    while ((line = reader.readLine()) != null) {
      // No setter should be found
      line = line.trim();
      assertFalse("No line should include the setter: " + line,
        line.startsWith("public void setValue("));
    }
    reader.close();
  }

  @Test
  public void testSettingOutputCharacterEncoding() throws Exception {
    SpecificCompiler compiler = createCompiler();
    // Generated file in default encoding
    compiler.compileToDestination(this.src, this.outputDir);
    byte[] fileInDefaultEncoding = new byte[(int) this.outputFile.length()];
    FileInputStream is = new FileInputStream(this.outputFile);
    is.read(fileInDefaultEncoding);
    is.close(); //close input stream otherwise delete might fail
    if (!this.outputFile.delete()) {
      throw new IllegalStateException("unable to delete " + this.outputFile); //delete otherwise compiler might not overwrite because src timestamp hasnt changed.
    }
    // Generate file in another encoding (make sure it has different number of bytes per character)
    String differentEncoding = Charset.defaultCharset().equals(Charset.forName("UTF-16")) ? "UTF-32" : "UTF-16";
    compiler.setOutputCharacterEncoding(differentEncoding);
    compiler.compileToDestination(this.src, this.outputDir);
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
  public void testJavaTypeWithDecimalLogicalTypeEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema dateSchema = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9,2)
        .addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid()
        .addToSchema(Schema.create(Schema.Type.STRING));

    // Date/time types should always use upper level java classes
    // Decimal type target class depends on configuration
    // UUID should always be CharSequence since we haven't added its
    // support in SpecificRecord
    Assert.assertEquals("Should use Joda LocalDate for date type",
        "org.joda.time.LocalDate", compiler.javaType(dateSchema));
    Assert.assertEquals("Should use Joda LocalTime for time-millis type",
        "org.joda.time.LocalTime", compiler.javaType(timeSchema));
    Assert.assertEquals("Should use Joda DateTime for timestamp-millis type",
        "org.joda.time.DateTime", compiler.javaType(timestampSchema));
    Assert.assertEquals("Should use Java BigDecimal type",
        "java.math.BigDecimal", compiler.javaType(decimalSchema));
    Assert.assertEquals("Should use Java CharSequence type",
        "java.lang.CharSequence", compiler.javaType(uuidSchema));
  }

  @Test
  public void testJavaTypeWithDecimalLogicalTypeDisabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    Schema dateSchema = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9,2)
        .addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid()
        .addToSchema(Schema.create(Schema.Type.STRING));

    // Date/time types should always use upper level java classes
    // Decimal type target class depends on configuration
    // UUID should always be CharSequence since we haven't added its
    // support in SpecificRecord
    Assert.assertEquals("Should use Joda LocalDate for date type",
        "org.joda.time.LocalDate", compiler.javaType(dateSchema));
    Assert.assertEquals("Should use Joda LocalTime for time-millis type",
        "org.joda.time.LocalTime", compiler.javaType(timeSchema));
    Assert.assertEquals("Should use Joda DateTime for timestamp-millis type",
        "org.joda.time.DateTime", compiler.javaType(timestampSchema));
    Assert.assertEquals("Should use ByteBuffer type",
        "java.nio.ByteBuffer", compiler.javaType(decimalSchema));
    Assert.assertEquals("Should use Java CharSequence type",
        "java.lang.CharSequence", compiler.javaType(uuidSchema));
  }

  @Test
  public void testJavaUnbox() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    Schema boolSchema = Schema.create(Schema.Type.BOOLEAN);

    Assert.assertEquals("Should use int for Type.INT",
        "int", compiler.javaUnbox(intSchema));
    Assert.assertEquals("Should use long for Type.LONG",
        "long", compiler.javaUnbox(longSchema));
    Assert.assertEquals("Should use float for Type.FLOAT",
        "float", compiler.javaUnbox(floatSchema));
    Assert.assertEquals("Should use double for Type.DOUBLE",
        "double", compiler.javaUnbox(doubleSchema));
    Assert.assertEquals("Should use boolean for Type.BOOLEAN",
        "boolean", compiler.javaUnbox(boolSchema));

    Schema dateSchema = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMicroSchema = LogicalTypes.timeMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampSchema = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMicrosSchema = LogicalTypes.timestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
    // Date/time types should always use upper level java classes, even though
    // their underlying representations are primitive types
    Assert.assertEquals("Should use Joda LocalDate for date type",
        "org.joda.time.LocalDate", compiler.javaUnbox(dateSchema));
    Assert.assertEquals("Should use Joda LocalTime for time-millis type",
        "org.joda.time.LocalTime", compiler.javaUnbox(timeSchema));
    Assert.assertEquals("Should use Joda DateTime for timestamp-millis type",
        "org.joda.time.DateTime", compiler.javaUnbox(timestampSchema));
    Assert.assertEquals("Should use Joda DateTime for timestamp-millis type",
        "org.joda.time.LocalTime", compiler.javaUnbox(timeMicroSchema));
    Assert.assertEquals("Should use Joda DateTime for timestamp-millis type",
        "org.joda.time.DateTime", compiler.javaUnbox(timestampMicrosSchema));
  }

  @Test
  public void testNullableTypesJavaUnbox() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    // Nullable types should return boxed types instead of primitive types
    Schema nullableIntSchema1 = Schema.createUnion(
        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    Schema nullableIntSchema2 = Schema.createUnion(
        Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableIntSchema1), "java.lang.Integer");
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableIntSchema2), "java.lang.Integer");

    Schema nullableLongSchema1 = Schema.createUnion(
        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    Schema nullableLongSchema2 = Schema.createUnion(
        Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableLongSchema1), "java.lang.Long");
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableLongSchema2), "java.lang.Long");

    Schema nullableFloatSchema1 = Schema.createUnion(
        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT));
    Schema nullableFloatSchema2 = Schema.createUnion(
        Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableFloatSchema1), "java.lang.Float");
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableFloatSchema2), "java.lang.Float");

    Schema nullableDoubleSchema1 = Schema.createUnion(
        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE));
    Schema nullableDoubleSchema2 = Schema.createUnion(
        Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableDoubleSchema1), "java.lang.Double");
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableDoubleSchema2), "java.lang.Double");

    Schema nullableBooleanSchema1 = Schema.createUnion(
        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN));
    Schema nullableBooleanSchema2 = Schema.createUnion(
        Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableBooleanSchema1), "java.lang.Boolean");
    Assert.assertEquals("Should return boxed type",
        compiler.javaUnbox(nullableBooleanSchema2), "java.lang.Boolean");
  }

  @Test
  public void testLogicalTypesWithMultipleFields() throws Exception {
    Schema logicalTypesWithMultipleFields = new Schema.Parser().parse(
        new File("src/test/resources/logical_types_with_multiple_fields.avsc"));
    assertCompilesWithJavaCompiler(
        new SpecificCompiler(logicalTypesWithMultipleFields).compile());
  }

  @Test
  public void testUnionAndFixedFields() throws Exception {
    Schema unionTypesWithMultipleFields = new Schema.Parser().parse(
        new File("src/test/resources/union_and_fixed_fields.avsc"));
    assertCompilesWithJavaCompiler(
        new SpecificCompiler(unionTypesWithMultipleFields).compile());
  }

  @Test
  public void testConversionInstanceWithDecimalLogicalTypeDisabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    Schema dateSchema = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMicroSchema = LogicalTypes.timeMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampSchema = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMicrosSchema = LogicalTypes.timestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9,2)
        .addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid()
        .addToSchema(Schema.create(Schema.Type.STRING));

    Assert.assertEquals("Should use DATE_CONVERSION for date type",
        "DATE_CONVERSION", compiler.conversionInstance(dateSchema));
    Assert.assertEquals("Should use TIME_CONVERSION for time type",
        "TIME_CONVERSION", compiler.conversionInstance(timeSchema));
    Assert.assertEquals("Should use TIME_MICROS_CONVERSION for time type",
        "TIME_MICROS_CONVERSION", compiler.conversionInstance(timeMicroSchema));
    Assert.assertEquals("Should use TIMESTAMP_CONVERSION for date type",
        "TIMESTAMP_CONVERSION", compiler.conversionInstance(timestampSchema));
    Assert.assertEquals("Should use TIMESTAMP_MICROS_CONVERSION for date type",
        "TIMESTAMP_MICROS_CONVERSION", compiler.conversionInstance(timestampMicrosSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off",
        "null", compiler.conversionInstance(decimalSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off",
        "null", compiler.conversionInstance(uuidSchema));

  }


  @Test
  public void testConversionInstanceWithDecimalLogicalTypeEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema dateSchema = LogicalTypes.date()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis()
        .addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMicroSchema = LogicalTypes.timeMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampSchema = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMicrosSchema = LogicalTypes.timestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9,2)
        .addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid()
        .addToSchema(Schema.create(Schema.Type.STRING));

    Assert.assertEquals("Should use DATE_CONVERSION for date type",
        "DATE_CONVERSION", compiler.conversionInstance(dateSchema));
    Assert.assertEquals("Should use TIME_CONVERSION for time type",
        "TIME_CONVERSION", compiler.conversionInstance(timeSchema));
    Assert.assertEquals("Should use TIME_MICROS_CONVERSION for time type",
        "TIME_MICROS_CONVERSION", compiler.conversionInstance(timeMicroSchema));
    Assert.assertEquals("Should use TIMESTAMP_CONVERSION for date type",
        "TIMESTAMP_CONVERSION", compiler.conversionInstance(timestampSchema));
    Assert.assertEquals("Should use TIMESTAMP_MICROS_CONVERSION for date type",
        "TIMESTAMP_MICROS_CONVERSION", compiler.conversionInstance(timestampMicrosSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off",
        "DECIMAL_CONVERSION", compiler.conversionInstance(decimalSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off",
        "null", compiler.conversionInstance(uuidSchema));
  }

  public void testToFromByteBuffer() {

  }
}
