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

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.compiler.specific.SpecificCompiler;

/**
 * A Tool for compiling avro protocols or schemas to Java classes using the Avro
 * SpecificCompiler.
 */

public class SpecificCompilerTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 3) {
      System.err
          .println("Expected 3 arguments: (schema|protocol) inputfile outputdir");
      return 1;
    }
    String method = args.get(0);
    File input = new File(args.get(1));
    File output = new File(args.get(2));
    if ("schema".equals(method)) {
      SpecificCompiler.compileSchema(input, output);
    } else if ("protocol".equals(method)) {
      SpecificCompiler.compileProtocol(input, output);
    } else {
      System.err.println("Expected \"schema\" or \"protocol\".");
      return 1;
    }
    return 0;
  }

  @Override
  public String getName() {
    return "compile";
  }

  @Override
  public String getShortDescription() {
    return "Generates Java code for the given schema.";
  }
}
