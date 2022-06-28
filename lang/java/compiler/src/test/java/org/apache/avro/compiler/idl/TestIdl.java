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

package org.apache.avro.compiler.idl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple test harness for Idl. This relies on an input/ and output/ directory.
 * Inside the input/ directory are .avdl files. Each file should have a
 * corresponding .avpr file in output/. When you run the test, it generates and
 * stringifies each .avdl file and compares it to the expected output, failing
 * if the two differ.
 *
 * To make it simpler to write these tests, you can run ant -Dtestcase=TestIdl
 * -Dtest.idl.mode=write, which will *replace* all expected output.
 */
public class TestIdl {
  private static final File TEST_DIR = new File(System.getProperty("test.idl.dir", "src/test/idl"));

  private static final File TEST_INPUT_DIR = new File(TEST_DIR, "input");

  private static final File TEST_OUTPUT_DIR = new File(TEST_DIR, "output");

  private static final String TEST_MODE = System.getProperty("test.idl.mode", "run");

  private List<GenTest> tests;

  @Before
  public void loadTests() {
    assertTrue(TEST_DIR.exists());
    assertTrue(TEST_INPUT_DIR.exists());
    assertTrue(TEST_OUTPUT_DIR.exists());

    tests = new ArrayList<>();
    for (File inF : requireNonNull(TEST_INPUT_DIR.listFiles())) {
      if (!inF.getName().endsWith(".avdl"))
        continue;
      if (inF.getName().startsWith("."))
        continue;

      File outF = new File(TEST_OUTPUT_DIR, inF.getName().replaceFirst("\\.avdl$", ".avpr"));
      tests.add(new GenTest(inF, outF));
    }
  }

  @Test
  public void runTests() throws Exception {
    if (!"run".equals(TEST_MODE))
      return;

    int failed = 0;

    for (GenTest t : tests) {
      try {
        t.run();
      } catch (Exception e) {
        failed++;
        System.err.println("Failed: " + t.testName());
        e.printStackTrace(System.err);
      }
    }

    if (failed > 0) {
      fail(failed + " tests failed");
    }
  }

  @Test
  public void writeTests() throws Exception {
    if (!"write".equals(TEST_MODE))
      return;

    for (GenTest t : tests) {
      t.write();
    }
  }

  @Test
  public void testDocCommentsAndWarnings() throws Exception {
    try (Idl parser = new Idl(new File(TEST_INPUT_DIR, "comments.avdl"))) {
      final Protocol protocol = parser.CompilationUnit();
      final List<String> warnings = parser.getWarningsAfterParsing();

      assertEquals("Documented Enum", protocol.getType("testing.DocumentedEnum").getDoc());

      assertEquals("Documented Fixed Type", protocol.getType("testing.DocumentedFixed").getDoc());

      final Schema documentedError = protocol.getType("testing.DocumentedError");
      assertEquals("Documented Error", documentedError.getDoc());
      assertEquals("Documented Reason Field", documentedError.getField("reason").doc());
      assertEquals("Default Doc Explanation Field", documentedError.getField("explanation").doc());

      final Map<String, Protocol.Message> messages = protocol.getMessages();
      final Protocol.Message documentedMethod = messages.get("documentedMethod");
      assertEquals("Documented Method", documentedMethod.getDoc());
      assertEquals("Documented Parameter", documentedMethod.getRequest().getField("message").doc());
      assertEquals("Default Documented Parameter", documentedMethod.getRequest().getField("defMsg").doc());

      assertNull(protocol.getType("testing.UndocumentedEnum").getDoc());
      assertNull(protocol.getType("testing.UndocumentedFixed").getDoc());
      assertNull(protocol.getType("testing.UndocumentedRecord").getDoc());
      assertNull(messages.get("undocumentedMethod").getDoc());

      final String pattern1 = "Found documentation comment at line %d, column %d. Ignoring previous one at line %d, column %d: \"%s\""
          + "\nDid you mean to use a multiline comment ( /* ... */ ) instead?";
      final String pattern2 = "Ignoring out-of-place documentation comment at line %d, column %d: \"%s\""
          + "\nDid you mean to use a multiline comment ( /* ... */ ) instead?";
      assertEquals(Arrays.asList(String.format(pattern1, 21, 47, 21, 10, "Dangling Enum1"),
          String.format(pattern2, 21, 47, "Dangling Enum2"), String.format(pattern1, 23, 9, 22, 9, "Dangling Enum3"),
          String.format(pattern1, 24, 9, 23, 9, "Dangling Enum4"),
          String.format(pattern1, 25, 5, 24, 9, "Dangling Enum5"), String.format(pattern2, 25, 5, "Dangling Enum6"),
          String.format(pattern1, 27, 5, 26, 5, "Dangling Enum7"),
          String.format(pattern1, 28, 5, 27, 5, "Dangling Enum8"), String.format(pattern2, 28, 5, "Dangling Enum9"),
          String.format(pattern1, 34, 5, 33, 5, "Dangling Fixed1"),
          String.format(pattern1, 35, 5, 34, 5, "Dangling Fixed2"),
          String.format(pattern1, 36, 5, 35, 5, "Dangling Fixed3"),
          String.format(pattern1, 37, 5, 36, 5, "Dangling Fixed4"), String.format(pattern2, 37, 5, "Dangling Fixed5"),
          String.format(pattern1, 43, 5, 42, 5, "Dangling Error1"), String.format(pattern2, 43, 5, "Dangling Field1"),
          String.format(pattern2, 46, 5, "Dangling Field2"), String.format(pattern2, 47, 5, "Dangling Error2"),
          String.format(pattern1, 55, 5, 54, 5, "Dangling Param1"), String.format(pattern2, 55, 5, "Dangling Param2"),
          String.format(pattern2, 58, 5, "Dangling Param3"), String.format(pattern1, 60, 5, 59, 5, "Dangling Method1"),
          String.format(pattern1, 61, 5, 60, 5, "Dangling Method2"),
          String.format(pattern2, 61, 5, "Dangling Method3")), warnings);
    }
  }

  /**
   * An individual comparison test
   */
  private static class GenTest {
    private final File in, expectedOut;

    public GenTest(File in, File expectedOut) {
      this.in = in;
      this.expectedOut = expectedOut;
    }

    private String generate() throws Exception {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();

      URL[] newPathURL = new URL[] { cl.getResource("putOnClassPath-test-resource.jar") };
      URLClassLoader ucl = new URLClassLoader(newPathURL, cl);

      Idl parser = new Idl(in, ucl);
      Protocol p = parser.CompilationUnit();
      parser.close();
      return p.toString();
    }

    public String testName() {
      return this.in.getName();
    }

    public void run() throws Exception {
      String output = generate();
      String slurped = slurp(expectedOut);
      assertEquals(slurped.trim(), output.replace("\\r", "").trim());
    }

    public void write() throws Exception {
      writeFile(expectedOut, generate());
    }

    private static String slurp(File f) throws IOException {
      BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8));

      String line;
      StringBuilder builder = new StringBuilder();
      while ((line = in.readLine()) != null) {
        builder.append(line);
      }
      in.close();
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(builder.toString());
      return mapper.writer().writeValueAsString(json);
    }

    private static void writeFile(File f, String s) throws IOException {
      FileWriter w = new FileWriter(f);
      w.write(s);
      w.close();
    }
  }
}
