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
package org.apache.avro.mojo;

import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test the Schema Mojo.
 */
public class TestSchemaMojo extends AbstractAvroMojoTest {

  private File testPom = new File(getBasedir(), "src/test/resources/unit/schema/pom.xml");
  private File injectingVelocityToolsTestPom = new File(getBasedir(),
      "src/test/resources/unit/schema/pom-injecting-velocity-tools.xml");
  private File testNonexistentFilePom = new File(getBasedir(),
      "src/test/resources/unit/schema/pom-nonexistent-file.xml");
  private File testNonexistentSecondFilePom = new File(getBasedir(),
      "src/test/resources/unit/schema/pom-nonexistent-second-file.xml");

  private File testExtendsFilePom = new File(getBasedir(), "src/test/resources/unit/schema/pom-customExtends.xml");

  @Test
  public void testSchemaMojo() throws Exception {
    final SchemaMojo mojo = (SchemaMojo) lookupMojo("schema", testPom);

    assertNotNull(mojo);
    mojo.execute();

    final File outputDir = new File(getBasedir(), "target/test-harness/schema/test");
    final Set<String> generatedFiles = new HashSet<>(Arrays.asList("PrivacyDirectImport.java", "PrivacyImport.java",
        "SchemaPrivacy.java", "SchemaUser.java", "SchemaCustom.java", "SchemaCustom.java"));

    assertFilesExist(outputDir, generatedFiles);

    final String schemaUserContent = FileUtils.fileRead(new File(outputDir, "SchemaUser.java"));
    assertTrue(schemaUserContent.contains("java.time.Instant"));
  }

  @Test
  public void testSetCompilerVelocityAdditionalTools() throws Exception {
    final SchemaMojo mojo = (SchemaMojo) lookupMojo("schema", injectingVelocityToolsTestPom);

    assertNotNull(mojo);
    mojo.execute();

    final File outputDir = new File(getBasedir(), "target/test-harness/schema-inject/test");
    final Set<String> generatedFiles = new HashSet<>(Arrays.asList("PrivacyDirectImport.java", "PrivacyImport.java",
        "SchemaPrivacy.java", "SchemaUser.java", "SchemaCustom.java"));

    assertFilesExist(outputDir, generatedFiles);

    final String schemaUserContent = FileUtils.fileRead(new File(outputDir, "SchemaUser.java"));
    assertTrue("Got " + schemaUserContent + " instead", schemaUserContent.contains("It works!"));
  }

  @Test
  public void testThrowsErrorForNonexistentFile() throws Exception {
    try {
      final SchemaMojo mojo = (SchemaMojo) lookupMojo("schema", testNonexistentFilePom);
      mojo.execute();
      fail("MojoExecutionException not thrown!");
    } catch (MojoExecutionException ignored) {
    }
  }

  @Test
  public void testThrowsErrorForNonexistentSecondFile() throws Exception {
    try {
      final SchemaMojo mojo = (SchemaMojo) lookupMojo("schema", testNonexistentSecondFilePom);
      mojo.execute();
      fail("MojoExecutionException not thrown!");
    } catch (MojoExecutionException ignored) {
    }
  }

  @Test
  public void testExtends() throws Exception {
    final SchemaMojo mojo = (SchemaMojo) lookupMojo("schema", testExtendsFilePom);
    assertNotNull(mojo);

    mojo.execute();
    final File outputDir = new File(getBasedir(), "target/extends/schema/test");
    File outputFile = new File(outputDir, "SchemaCustom.java");
    assertTrue(outputFile.exists());
    List<String> extendsLines = Files.readAllLines(outputFile.toPath()).stream()
        .filter((String line) -> line.contains("class SchemaCustom extends ")).collect(Collectors.toList());
    assertEquals(1, extendsLines.size());
    String extendLine = extendsLines.get(0);
    assertTrue(extendLine.contains(" org.apache.avro.custom.CustomRecordBase "));
    assertFalse(extendLine.contains("org.apache.avro.specific.SpecificRecordBase"));
  }
}
