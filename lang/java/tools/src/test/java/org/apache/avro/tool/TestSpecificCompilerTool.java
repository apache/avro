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
package org.apache.avro.tool;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * Verifies that the SpecificCompilerTool generates Java source properly
 */
public class TestSpecificCompilerTool {

  // where test input/expected output comes from
  private static final File TEST_DIR =
    new File(System.getProperty("test.compile.schema.dir", "src/test/compiler"));

  // where test input comes from
  private static final File TEST_INPUT_DIR =
    new File(TEST_DIR, "input");

  // where test expected output comes from
  private static final File TEST_EXPECTED_OUTPUT_DIR =
    new File(TEST_DIR, "output");
  private static final File TEST_EXPECTED_POSITION =
    new File(TEST_EXPECTED_OUTPUT_DIR, "Position.java");
  private static final File TEST_EXPECTED_PLAYER =
    new File(TEST_EXPECTED_OUTPUT_DIR, "Player.java");

  // where test output goes
  private static final File TEST_OUTPUT_DIR =
    new File("target/compiler/output");
  private static final File TEST_OUTPUT_PLAYER =
    new File(TEST_OUTPUT_DIR, "avro/examples/baseball/Player.java");
  private static final File TEST_OUTPUT_POSITION =
    new File(TEST_OUTPUT_DIR, "avro/examples/baseball/Position.java");

  private static final File TEST_OUTPUT_STRING_DIR =
    new File("target/compiler/output-string");
  private static final File TEST_OUTPUT_STRING_PLAYER =
    new File(TEST_OUTPUT_DIR, "avro/examples/baseball/Player.java");
  private static final File TEST_OUTPUT_STRING_POSITION =
    new File(TEST_OUTPUT_DIR, "avro/examples/baseball/Position.java");

  @Before
  public void setUp() {
    TEST_OUTPUT_DIR.delete();
  }

  @Test
  public void testCompileSchemaSingleFile() throws Exception {

    doCompile(new String[]{"schema",
      TEST_INPUT_DIR.toString() + "/position.avsc",
      TEST_OUTPUT_DIR.getPath()});
    assertFileMatch(TEST_EXPECTED_POSITION, TEST_OUTPUT_POSITION);
  }

  @Test
  public void testCompileSchemaTwoFiles() throws Exception {

    doCompile(new String[]{"schema",
      TEST_INPUT_DIR.toString() + "/position.avsc",
      TEST_INPUT_DIR.toString() + "/player.avsc",
      TEST_OUTPUT_DIR.getPath()});
    assertFileMatch(TEST_EXPECTED_POSITION, TEST_OUTPUT_POSITION);
    assertFileMatch(TEST_EXPECTED_PLAYER,   TEST_OUTPUT_PLAYER);
  }

  @Test
  public void testCompileSchemaFileAndDirectory() throws Exception {

    doCompile(new String[]{"schema",
      TEST_INPUT_DIR.toString() + "/position.avsc",
      TEST_INPUT_DIR.toString(),
      TEST_OUTPUT_DIR.getPath()});
    assertFileMatch(TEST_EXPECTED_POSITION, TEST_OUTPUT_POSITION);
    assertFileMatch(TEST_EXPECTED_PLAYER,   TEST_OUTPUT_PLAYER);
  }

  @Test
  public void testCompileSchemasUsingString() throws Exception {

    doCompile(new String[]{"-string", "schema",
      TEST_INPUT_DIR.toString() + "/position.avsc",
      TEST_INPUT_DIR.toString() + "/player.avsc",
      TEST_OUTPUT_DIR.getPath()});
    assertFileMatch(TEST_EXPECTED_POSITION, TEST_OUTPUT_STRING_POSITION);
    assertFileMatch(TEST_EXPECTED_PLAYER,   TEST_OUTPUT_STRING_PLAYER);
  }

  // Runs the actual compiler tool with the given input args
  private void doCompile(String[] args) throws Exception {
    SpecificCompilerTool tool = new SpecificCompilerTool();
    tool.run(null, null, null, Arrays.asList((args)));
  }

  /**
   * Verify that the generated Java files match the expected. This approach has
   * room for improvement, since we're currently just verify that the text matches,
   * which can be brittle if the code generation formatting or method ordering
   * changes for example. A better approach would be to compile the sources and
   * do a deeper comparison.
   *
   * See http://download.oracle.com/javase/6/docs/api/javax/tools/JavaCompiler.html
   */
  private static void assertFileMatch(File expected, File found) throws IOException {
    Assert.assertEquals("Found file: " + found +
      " does not match expected file: " + expected,
      readFile(expected), readFile(found));
  }

  /**
   * Not the best implementation, but does the job. Building full strings of the
   * file content and comparing provides nice diffs via JUnit when failures occur.
   */
  private static String readFile(File file) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(file));
    StringBuilder sb = new StringBuilder();
    String line = null;
    boolean first = true;
    while ((line = reader.readLine()) != null) {
      if (!first) {
        sb.append("\n");
        first = false;
      }
      sb.append(line);
    }
    return sb.toString();
  }
}
