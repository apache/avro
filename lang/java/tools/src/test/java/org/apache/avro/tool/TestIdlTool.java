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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestIdlTool {


  @Test
  public void testIdl() throws Exception {
    String idl = "src/test/idl/protocol.avdl";
    String outdir = "target/test-idl";

    List<String> argList = Arrays.asList(idl, outdir);
    IdlTool tool = new IdlTool();
    int rc = tool.run(System.in, System.out, System.err, argList);
    assertEquals(0, rc);
  }

  @Test
  public void testIncorrectArgs() throws Exception {
    List<String> argList = Arrays.asList("non-existing-file");
    IdlTool tool = new IdlTool();
    int rc = tool.run(System.in, System.out, System.err, argList);
    assertEquals(-1, rc);
  }

  @Test
  public void testIncludeDir() throws Exception {
    String idl = "src/test/idl/incltest.avdl";
    String includeDir = "src/test/idl/incldir";
    String outdir = "target/test-idl-includedir";

    List<String> argList = Arrays.asList("--include", includeDir, idl, outdir);
    IdlTool tool = new IdlTool();
    int rc = tool.run(System.in, System.out, System.err, argList);
    assertEquals(0, rc);
  }

  @Test
  public void testStdInStdOutArgs() throws Exception {
    String idl = "src/test/idl/protocol.avdl";
    String result = "target/idltool-stdin-stdout-test";
    FileInputStream input = new FileInputStream(idl);
    PrintStream output = new PrintStream(result);
    List<String> argList = Arrays.asList();

    IdlTool tool = new IdlTool();
    int rc = tool.run(input, output, System.err, argList);
    assertEquals(0, rc);
  }
}
