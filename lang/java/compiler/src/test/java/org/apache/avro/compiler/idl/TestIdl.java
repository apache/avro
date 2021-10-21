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
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple test harness for Idl. This relies on an input/ and output/ directory.
 * Inside the input/ directory are .avdl files. Each file should have a
 * corresponding .avpr file in output/. When the test is run, it generates and
 * stringifies each .avdl file and compares it to the expected output, failing
 * if the two differ.
 *
 * To make it simpler to write these tests, you can run ant -Dtestcase=TestIdl
 * -Dtest.idl.mode=write which will *replace* all expected output.
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
    for (File inF : TEST_INPUT_DIR.listFiles()) {
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
      fail(String.valueOf(failed) + " tests failed");
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
      assertEquals("Dangling Enum9", protocol.getType("testing.NotUndocumentedEnum").getDoc()); // Arguably a bug
      assertEquals("Documented Fixed Type", protocol.getType("testing.DocumentedFixed").getDoc());
      assertNull(protocol.getType("testing.UndocumentedFixed").getDoc());
      final Schema documentedError = protocol.getType("testing.DocumentedError");
      assertEquals("Documented Error", documentedError.getDoc());
      assertEquals("Documented Field", documentedError.getField("reason").doc());
      assertEquals("Dangling Error2", protocol.getType("testing.NotUndocumentedRecord").getDoc()); // Arguably a bug
      final Map<String, Protocol.Message> messages = protocol.getMessages();
      assertEquals("Documented Method", messages.get("documentedMethod").getDoc());
      assertEquals("Documented Parameter", messages.get("documentedMethod").getRequest().getField("message").doc());
      assertEquals("Dangling Method3", messages.get("notUndocumentedMethod").getDoc()); // Arguably a bug

      assertEquals(23, warnings.size());
      final String pattern = "Found documentation comment at line %d, column %d. Ignoring previous one at line %d, column %d: \"%s\""
          + "\nA common cause is to use documentation comments ( /** ... */ ) instead of multiline comments ( /* ... */ ).";
      assertEquals(String.format(pattern, 4, 47, 4, 10, "Dangling Enum1"), warnings.get(0));
      assertEquals(String.format(pattern, 5, 9, 4, 47, "Dangling Enum2"), warnings.get(1));
      assertEquals(String.format(pattern, 6, 9, 5, 9, "Dangling Enum3"), warnings.get(2));
      assertEquals(String.format(pattern, 7, 9, 6, 9, "Dangling Enum4"), warnings.get(3));
      assertEquals(String.format(pattern, 8, 5, 7, 9, "Dangling Enum5"), warnings.get(4));
      assertEquals(String.format(pattern, 9, 5, 8, 5, "Dangling Enum6"), warnings.get(5));
      assertEquals(String.format(pattern, 10, 5, 9, 5, "Dangling Enum7"), warnings.get(6));
      assertEquals(String.format(pattern, 11, 5, 10, 5, "Dangling Enum8"), warnings.get(7));
      assertEquals(String.format(pattern, 17, 5, 16, 5, "Dangling Fixed1"), warnings.get(8));
      assertEquals(String.format(pattern, 18, 5, 17, 5, "Dangling Fixed2"), warnings.get(9));
      assertEquals(String.format(pattern, 19, 5, 18, 5, "Dangling Fixed3"), warnings.get(10));
      assertEquals(String.format(pattern, 20, 5, 19, 5, "Dangling Fixed4"), warnings.get(11));
      assertEquals(String.format(pattern, 21, 5, 20, 5, "Dangling Fixed5"), warnings.get(12));
      assertEquals(String.format(pattern, 26, 5, 25, 5, "Dangling Error1"), warnings.get(13));
      assertEquals(String.format(pattern, 28, 5, 27, 5, "Dangling Field1"), warnings.get(14));
      assertEquals(String.format(pattern, 29, 5, 28, 5, "Dangling Field2"), warnings.get(15));
      assertEquals(String.format(pattern, 30, 5, 29, 5, "Dangling Field3"), warnings.get(16));
      assertEquals(String.format(pattern, 40, 5, 39, 5, "Dangling Param1"), warnings.get(17));
      assertEquals(String.format(pattern, 41, 9, 40, 5, "Dangling Param2"), warnings.get(18));
      assertEquals(String.format(pattern, 42, 9, 41, 9, "Dangling Param3"), warnings.get(19));
      assertEquals(String.format(pattern, 43, 5, 42, 9, "Dangling Param4"), warnings.get(20));
      assertEquals(String.format(pattern, 45, 5, 44, 5, "Dangling Method1"), warnings.get(21));
      assertEquals(String.format(pattern, 46, 5, 45, 5, "Dangling Method2"), warnings.get(22));
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
