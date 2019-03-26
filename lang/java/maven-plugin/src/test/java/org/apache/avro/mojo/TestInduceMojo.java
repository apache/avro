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

import java.io.File;

import org.apache.maven.plugin.testing.AbstractMojoTestCase;

public class TestInduceMojo extends AbstractMojoTestCase {

  protected File schemaPom;
  protected File protocolPom;

  @Override
  protected void setUp() throws Exception {
    String baseDir = getBasedir();
    schemaPom = new File(baseDir, "src/test/resources/unit/schema/induce-pom.xml");
    protocolPom = new File(baseDir, "src/test/resources/unit/protocol/induce-pom.xml");
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testInduceMojoExists() throws Exception {
    InduceMojo mojo = (InduceMojo) lookupMojo("induce", schemaPom);

    assertNotNull(mojo);
  }

  public void testInduceSchema() throws Exception {
    executeMojo(schemaPom);

    File outputDir = new File(getBasedir(), "target/test-harness/schemas/org/apache/avro/entities");
    assertTrue(outputDir.listFiles().length != 0);
  }

  public void testInducedSchemasFileExtension() throws Exception {
    executeMojo(schemaPom);

    File outputDir = new File(getBasedir(), "target/test-harness/schemas/org/apache/avro/entities");
    for (File file : outputDir.listFiles()) {
      assertTrue(file.getName().contains(".avsc"));
    }
  }

  public void testInduceProtocol() throws Exception {
    executeMojo(protocolPom);

    File outputDir = new File(getBasedir(), "target/test-harness/protocol/org/apache/avro/protocols");
    assertTrue(outputDir.listFiles().length != 0);
  }

  public void testInducedProtocolsFileExtension() throws Exception {
    executeMojo(protocolPom);

    File outputDir = new File(getBasedir(), "target/test-harness/protocol/org/apache/avro/protocols");
    for (File file : outputDir.listFiles()) {
      assertTrue(file.getName().contains(".avpr"));
    }
  }

  private void executeMojo(File pom) throws Exception {
    InduceMojo mojo = (InduceMojo) lookupMojo("induce", pom);
    mojo.execute();
  }
}
