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
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Test the IDL 2 Schema Mojo.
 */
public class TestIDL2SchemaMojo extends AbstractAvroMojoTest {
  private File testPom = new File(getBasedir(), "src/test/resources/unit/idl/pom-idl2schema.xml");

  @Test
  public void testGenerateSchemaTools() throws Exception {
    final IDL2SchemaMojo mojo = (IDL2SchemaMojo) lookupMojo("idl2schema", testPom);
    final TestLog log = new TestLog();
    mojo.setLog(log);

    assertNotNull(mojo);
    mojo.execute();

    final File outputDir = new File(getBasedir(), "target/test-harness/idl2schema/test");
    final Set<String> generatedFiles = new HashSet<>(asList("IdlPrivacy.avsc", "IdlUser.avsc", "IdlUserWrapper.avsc"));

    assertFilesExist(outputDir, generatedFiles);

    final String schemaUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.avsc"));
    assertTrue(schemaUserContent.contains("IdlUser document"));
    assertTrue(schemaUserContent.contains("Private"));
    assertTrue(schemaUserContent.contains("pii"));

    // The previous test already verifies the warnings.
    assertFalse(log.getLogEntries().isEmpty());
  }
}
