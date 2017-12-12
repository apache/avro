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
package org.apache.avro.mojo;

import org.codehaus.plexus.util.FileUtils;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Test the IDL Protocol Mojo.
 *
 * @author saden
 */
public class TestIDLProtocolMojo extends AbstractAvroMojoTest {

  protected File jodaTestPom = new File(getBasedir(),
          "src/test/resources/unit/idl/pom-joda.xml");
  protected File java8TestPom = new File(getBasedir(),
          "src/test/resources/unit/idl/pom-java8.xml");

  public void testIdlProtocolMojoJoda() throws Exception {
    IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", jodaTestPom);

    assertNotNull(mojo);
    mojo.execute();

    File outputDir = new File(getBasedir(), "target/test-harness/idl-joda/test");
    String[] generatedFileNames = new String[]{"IdlPrivacy.java",
      "IdlTest.java", "IdlUser.java", "IdlUserWrapper.java"};

    String idlUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(idlUserContent.contains("org.joda.time.DateTime"));
  }

  public void testIdlProtocolMojoJava8() throws Exception {
    IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", java8TestPom);

    assertNotNull(mojo);
    mojo.execute();

    File outputDir = new File(getBasedir(), "target/test-harness/idl-java8/test");
    String[] generatedFileNames = new String[]{"IdlPrivacy.java",
      "IdlTest.java", "IdlUser.java", "IdlUserWrapper.java"};

    String idlUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(idlUserContent.contains("java.time.Instant"));
  }
}
