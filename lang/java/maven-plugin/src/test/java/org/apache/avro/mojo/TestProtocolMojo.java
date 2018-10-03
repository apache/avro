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
package org.apache.avro.mojo;

import org.codehaus.plexus.util.FileUtils;

import java.io.File;

/**
 * Test the Protocol Mojo.
 *
 * @author saden
 */
public class TestProtocolMojo extends AbstractAvroMojoTest {

  protected File jodaTestPom = new File(getBasedir(),
          "src/test/resources/unit/protocol/pom-joda.xml");
  protected File jsr310TestPom = new File(getBasedir(),
          "src/test/resources/unit/protocol/pom-jsr310.xml");

  public void testProtocolMojoJoda() throws Exception {
    ProtocolMojo mojo = (ProtocolMojo) lookupMojo("protocol", jodaTestPom);

    assertNotNull(mojo);
    mojo.execute();

    File outputDir = new File(getBasedir(), "target/test-harness/protocol-joda/test");
    String[] generatedFiles = new String[]{"ProtocolPrivacy.java",
      "ProtocolTest.java", "ProtocolUser.java"};

    assertFilesExist(outputDir, generatedFiles);

    String protocolUserContent = FileUtils.fileRead(new File(outputDir, "ProtocolUser.java"));
    assertTrue(protocolUserContent.contains("org.joda.time.DateTime"));
  }

  public void testProtocolMojoJsr310() throws Exception {
    ProtocolMojo mojo = (ProtocolMojo) lookupMojo("protocol", jsr310TestPom);

    assertNotNull(mojo);
    mojo.execute();

    File outputDir = new File(getBasedir(), "target/test-harness/protocol-jsr310/test");
    String[] generatedFiles = new String[]{"ProtocolPrivacy.java",
            "ProtocolTest.java", "ProtocolUser.java"};

    assertFilesExist(outputDir, generatedFiles);

    String protocolUserContent = FileUtils.fileRead(new File(outputDir, "ProtocolUser.java"));
    assertTrue(protocolUserContent.contains("java.time.Instant"));
  }
}
