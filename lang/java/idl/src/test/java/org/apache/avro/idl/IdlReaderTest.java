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
package org.apache.avro.idl;

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
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple test harness for Idl. This relies on an input/ and output/ directory.
 * Inside the input/ directory are .avdl files. Each file should have a
 * corresponding .avpr file in output/. When the test runs, it generates and
 * stringifies each .avdl file and compares it to the expected output, failing
 * if the two differ.
 * <p>
 * To make it simpler to write these tests, you can run ant -Dtestcase=TestIdl
 * -Dtest.idl.mode=write, which will *replace* all expected output.
 */
public class IdlReaderTest {
  private static final File TEST_DIR = new File(System.getProperty("test.idl.dir", "src/test/idl"));

  private static final File TEST_INPUT_DIR = new File(TEST_DIR, "input").getAbsoluteFile();

  private static final File TEST_OUTPUT_DIR = new File(TEST_DIR, "output");

  private static final String TEST_MODE = System.getProperty("test.idl.mode", "run");

  private static final File EXTRA_TEST_DIR = new File(TEST_DIR, "extra");

  private List<GenTest> tests;

  @Before
  public void loadTests() {
    assertTrue(TEST_DIR.exists());
    assertTrue(TEST_INPUT_DIR.exists());
    assertTrue(TEST_OUTPUT_DIR.exists());

    tests = new ArrayList<>();
    for (File inF : Objects.requireNonNull(TEST_INPUT_DIR.listFiles())) {
      if (!inF.getName().endsWith(".avdl")) {
        continue;
      }
      if (inF.getName().startsWith(".")) {
        continue;
      }

      File outF = new File(TEST_OUTPUT_DIR,
          inF.getName().replaceFirst("_schema\\.avdl$", ".avsc").replaceFirst("\\.avdl$", ".avpr"));
      tests.add(new GenTest(inF, outF));
    }
  }

  @Test
  public void validateProtocolParsingResult() throws IOException {
    // runTests already tests the actual parsing; this tests the result object.
    IdlFile idlFile = parseExtraIdlFile("protocolSyntax.avdl");

    assertEquals(1, idlFile.getNamedSchemas().size());
    idlFile.getNamedSchemas().keySet().forEach(System.out::println);
    assertNotNull(idlFile.getNamedSchema("communication.Message"));
    assertNotNull(idlFile.getNamedSchema("Message"));

    assertNotNull(idlFile.getProtocol());
  }

  @Test
  public void testDocCommentsAndWarnings() throws Exception {
    final IdlFile idlFile = parseExtraIdlFile("../input/comments.avdl");
    final Protocol protocol = idlFile.getProtocol();
    final List<String> warnings = idlFile.getWarnings();

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

    final String pattern = "Line %d, char %d: Ignoring out-of-place documentation comment.%n"
        + "Did you mean to use a multiline comment ( /* ... */ ) instead?";
    assertEquals(
        Arrays.asList(String.format(pattern, 21, 8), String.format(pattern, 21, 45), String.format(pattern, 22, 5),
            String.format(pattern, 23, 5), String.format(pattern, 24, 5), String.format(pattern, 25, 5),
            String.format(pattern, 26, 7), String.format(pattern, 27, 7), String.format(pattern, 28, 7),
            String.format(pattern, 33, 7), String.format(pattern, 34, 7), String.format(pattern, 35, 5),
            String.format(pattern, 36, 5), String.format(pattern, 37, 7), String.format(pattern, 42, 7),
            String.format(pattern, 43, 7), String.format(pattern, 46, 9), String.format(pattern, 47, 5),
            String.format(pattern, 54, 7), String.format(pattern, 55, 7), String.format(pattern, 58, 9),
            String.format(pattern, 59, 7), String.format(pattern, 60, 11), String.format(pattern, 61, 11)),
        warnings);
  }

  @SuppressWarnings("SameParameterValue")
  private IdlFile parseExtraIdlFile(String fileName) throws IOException {
    return new IdlReader().parse(EXTRA_TEST_DIR.toPath().resolve(fileName));
  }

  @Test
  public void runTests() {
    if (!"run".equals(TEST_MODE)) {
      return;
    }

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
    if (!"write".equals(TEST_MODE)) {
      return;
    }

    for (GenTest t : tests) {
      t.write();
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

    private String generate() {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();

      URL[] newPathURL = new URL[] { cl.getResource("putOnClassPath-test-resource.jar") };
      URLClassLoader ucl = new URLClassLoader(newPathURL, cl);
      Thread.currentThread().setContextClassLoader(ucl);
      try {
        IdlReader parser = new IdlReader();
        return parser.parse(in.toPath()).outputString();
      } catch (IOException e) {
        throw new AssertionError(e.getMessage(), e);
      } finally {
        Thread.currentThread().setContextClassLoader(cl);
      }
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
