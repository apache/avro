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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.plexus.util.FileUtils;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 * Test the IDL Protocol Mojo.
 */
public class TestIDLMojo extends AbstractAvroMojoTest {

  private File testPom = new File(getBasedir(), "src/test/resources/unit/idl/pom.xml");
  private File injectingVelocityToolsTestPom = new File(getBasedir(),
      "src/test/resources/unit/idl/pom-injecting-velocity-tools.xml");

  private File incrementalCompilationTestPom = new File(getBasedir(),
      "src/test/resources/unit/idl/pom-incremental-compilation.xml");

  @Test
  public void testIdlProtocolMojo() throws Exception {
    // Clear output directory to ensure files are recompiled.
    final File outputDir = new File(getBasedir(), "target/test-harness/idl/test/");
    FileUtils.deleteDirectory(outputDir);

    final IDLMojo mojo = (IDLMojo) lookupMojo("idl", testPom);
    final TestLog log = new TestLog();
    mojo.setLog(log);

    assertNotNull(mojo);
    mojo.execute();

    final Set<String> generatedFiles = new HashSet<>(
        asList("IdlPrivacy.java", "IdlTest.java", "IdlUser.java", "IdlUserWrapper.java"));
    assertFilesExist(outputDir, generatedFiles);

    final String idlUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(idlUserContent.contains("@org.jetbrains.annotations.Nullable\n  public java.lang.String getId"));
    assertTrue(idlUserContent.contains("@org.jetbrains.annotations.NotNull\n  public java.time.Instant getModifiedOn"));

    assertEquals(Collections.singletonList("[WARN] Line 22, char 1: Ignoring out-of-place documentation comment.\n"
        + "Did you mean to use a multiline comment ( /* ... */ ) instead?"), log.getLogEntries());
  }

  @Test
  public void testSetCompilerVelocityAdditionalTools() throws Exception {
    // Clear output directory to ensure files are recompiled.
    final File outputDir = new File(getBasedir(), "target/test-harness/idl-inject/test/");
    FileUtils.deleteDirectory(outputDir);

    final IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", injectingVelocityToolsTestPom);
    final TestLog log = new TestLog();
    mojo.setLog(log);

    assertNotNull(mojo);
    mojo.execute();

    final Set<String> generatedFiles = new HashSet<>(
        asList("IdlPrivacy.java", "IdlTest.java", "IdlUser.java", "IdlUserWrapper.java"));

    assertFilesExist(outputDir, generatedFiles);

    final String schemaUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(schemaUserContent.contains("It works!"));

    // The previous test already verifies the warnings.
    assertFalse(log.getLogEntries().isEmpty());
  }

  @Test
  public void testIDLProtocolMojoSupportsIncrementalCompilation() throws Exception {
    // Ensure that the IDL files have already been compiled once.
    final IDLMojo mojo = (IDLMojo) lookupMojo("idl", incrementalCompilationTestPom);
    final TestLog log = new TestLog();
    mojo.setLog(log);

    assertNotNull(mojo);
    mojo.execute();

    // Remove one file to ensure it is recreated and the others are not.
    final Path outputDirPath = Paths.get(getBasedir(), "target/test-harness/idl-incremental/test/");
    final File outputDir = outputDirPath.toFile();

    final Path idlPrivacyFilePath = outputDirPath.resolve("IdlPrivacy.java");
    final FileTime idpPrivacyModificationTime = Files.getLastModifiedTime(idlPrivacyFilePath);
    Files.delete(idlPrivacyFilePath);

    final Path idlUserFilePath = outputDirPath.resolve("IdlUser.java");
    final FileTime idlUserModificationTime = Files.getLastModifiedTime(idlUserFilePath);

    mojo.execute();

    // Asserting contents is done in previous tests so just assert existence.
    final Set<String> generatedFiles = new HashSet<>(
        asList("IdlPrivacy.java", "IdlTest.java", "IdlUser.java", "IdlUserWrapper.java"));
    assertFilesExist(outputDir, generatedFiles);

    assertTrue(idlPrivacyFilePath.toFile().exists());
    assertEquals(Files.getLastModifiedTime(idlUserFilePath), idlUserModificationTime);
    assertTrue(Files.getLastModifiedTime(idlPrivacyFilePath).compareTo(idpPrivacyModificationTime) > 0);
  }
}
