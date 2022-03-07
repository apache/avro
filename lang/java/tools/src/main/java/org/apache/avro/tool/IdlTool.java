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

package org.apache.avro.tool;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.idl.IdlFile;
import org.apache.avro.idl.IdlReader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * Tool implementation for generating Avro JSON schemata from idl format files.
 */
public class IdlTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {

    boolean useJavaCC = "--useJavaCC".equals(getArg(args, 0, null));
    if (args.size() > (useJavaCC ? 3 : 2)
        || (args.size() == 1 && (args.get(0).equals("--help") || args.get(0).equals("-help")))) {
      err.println("Usage: idl [--useJavaCC] [in [out]]");
      err.println();
      err.println("If an output path is not specified, outputs to stdout.");
      err.println("If no input or output is specified, takes input from");
      err.println("stdin and outputs to stdout.");
      err.println("The special path \"-\" may also be specified to refer to");
      err.println("stdin and stdout.");
      return -1;
    }

    String inputName = getArg(args, useJavaCC ? 1 : 0, "-");
    File inputFile = "-".equals(inputName) ? null : new File(inputName);
    String outputName = getArg(args, useJavaCC ? 2 : 1, "-");
    File outputFile = "-".equals(outputName) ? null : new File(outputName);

    Schema m = null;
    Protocol p = null;
    if (useJavaCC) {
      try (Idl parser = new Idl(inputFile)) {
        p = parser.CompilationUnit();
        for (String warning : parser.getWarningsAfterParsing()) {
          err.println("Warning: " + warning);
        }
      }
    } else {
      IdlReader parser = new IdlReader();
      IdlFile idlFile = inputFile == null ? parser.parse(in) : parser.parse(inputFile.toPath());
      for (String warning : idlFile.getWarnings()) {
        err.println("Warning: " + warning);
      }
      p = idlFile.getProtocol();
      m = idlFile.getMainSchema();
    }

    PrintStream parseOut = out;
    if (outputFile != null) {
      parseOut = new PrintStream(new FileOutputStream(outputFile));
    }

    if (m == null && p == null) {
      err.println("Error: the IDL file does not contain a schema nor a protocol.");
      return 1;
    }
    try {
      parseOut.print(m == null ? p.toString(true) : m.toString(true));
    } finally {
      if (parseOut != out) // Close only the newly created FileOutputStream
        parseOut.close();
    }
    return 0;
  }

  private String getArg(List<String> args, int index, String defaultValue) {
    if (index < args.size()) {
      return args.get(index);
    } else {
      return defaultValue;
    }
  }

  @Override
  public String getName() {
    return "idl";
  }

  @Override
  public String getShortDescription() {
    return "Generates a JSON schema or protocol from an Avro IDL file";
  }
}
