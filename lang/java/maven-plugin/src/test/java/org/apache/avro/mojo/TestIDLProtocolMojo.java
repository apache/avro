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

import org.codehaus.plexus.util.FileUtils;

import java.io.File;

/**
 * Test the IDL Protocol Mojo.
 *
 * @author saden
 */
public class TestIDLProtocolMojo extends AbstractAvroMojoTest {

  protected File jodaTestPom = new File(getBasedir(), "src/test/resources/unit/idl/pom-joda.xml");
  protected File jsr310TestPom = new File(getBasedir(), "src/test/resources/unit/idl/pom-jsr310.xml");
  protected File injectingVelocityToolsTestPom = new File(getBasedir(),
      "src/test/resources/unit/idl/pom-injecting-velocity-tools.xml");

  public void testIdlProtocolMojoJoda() throws Exception {
    IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", jodaTestPom);

    assertNotNull(mojo);
    mojo.execute();

    File outputDir = new File(getBasedir(), "target/test-harness/idl-joda/test");
    String[] generatedFileNames = new String[] { "IdlPrivacy.java", "IdlTest.java", "IdlUser.java",
        "IdlUserWrapper.java" };

    String idlUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(idlUserContent.contains("org.joda.time.DateTime"));
  }

  public void testIdlProtocolMojoJsr310() throws Exception {
    IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", jsr310TestPom);

    assertNotNull(mojo);
    mojo.execute();

    File outputDir = new File(getBasedir(), "target/test-harness/idl-jsr310/test");
    String[] generatedFileNames = new String[] { "IdlPrivacy.java", "IdlTest.java", "IdlUser.java",
        "IdlUserWrapper.java" };

    String idlUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(idlUserContent.contains("java.time.Instant"));
  }

  public void testSetCompilerVelocityAdditionalTools() throws Exception {
    injectingVelocityToolsTestPom = new File(getBasedir(),
        "src/test/resources/unit/idl/pom-injecting-velocity-tools.xml");
    IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", injectingVelocityToolsTestPom);

    assertNotNull(mojo);
    mojo.execute();

    File outputDir = new File(getBasedir(), "target/test-harness/idl/test");
    String[] generatedFiles = new String[] { "IdlPrivacy.java", "IdlTest.java", "IdlUser.java", "IdlUserWrapper.java" };

    assertFilesExist(outputDir, generatedFiles);

    String schemaUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(schemaUserContent.contains("It works!"));
  }
}
