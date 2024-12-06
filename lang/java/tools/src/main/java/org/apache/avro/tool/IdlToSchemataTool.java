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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * Extract the Avro JSON schemata of the types of a protocol defined through an
 * idl format file.
 */
public class IdlToSchemataTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    boolean useJavaCC = "--useJavaCC".equals(getArg(args, 0, null));

    if (args.isEmpty() || args.size() > (useJavaCC ? 3 : 2) || isRequestingHelp(args)) {
      err.println("Usage: idl2schemata [--useJavaCC] [idl [outdir]]");
      err.println();
      err.println("If an output directory is not specified, " + "outputs to current directory.");
      return -1;
    }

    String inputName = getArg(args, useJavaCC ? 1 : 0, "-");
    File inputFile = "-".equals(inputName) ? null : new File(inputName);
    File outputDirectory = getOutputDirectory(getArg(args, useJavaCC ? 2 : 1, ""));

    if (useJavaCC) {
      try (Idl parser = new Idl(inputFile)) {
        final Protocol protocol = parser.CompilationUnit();
        final List<String> warnings = parser.getWarningsAfterParsing();
        for (String warning : warnings) {
          err.println("Warning: " + warning);
        }
        for (Schema schema : protocol.getTypes()) {
          print(schema, outputDirectory);
        }
      }
    } else {
      IdlReader parser = new IdlReader();
      IdlFile idlFile = inputFile == null ? parser.parse(in) : parser.parse(inputFile.toPath());
      for (String warning : idlFile.getWarnings()) {
        err.println("Warning: " + warning);
      }
      for (Schema schema : idlFile.getNamedSchemas().values()) {
        print(schema, outputDirectory);
      }
    }

    return 0;
  }

  private boolean isRequestingHelp(List<String> args) {
    return args.size() == 1 && (args.get(0).equals("--help") || args.get(0).equals("-help"));
  }

  private String getArg(List<String> args, int index, String defaultValue) {
    if (index < args.size()) {
      return args.get(index);
    } else {
      return defaultValue;
    }
  }

  private File getOutputDirectory(String dirname) {
    File outputDirectory = new File(dirname);
    outputDirectory.mkdirs();
    return outputDirectory;
  }

  private void print(Schema schema, File outputDirectory) throws FileNotFoundException {
    String dirpath = outputDirectory.getAbsolutePath();
    String filename = dirpath + "/" + schema.getName() + ".avsc";
    FileOutputStream fileOutputStream = new FileOutputStream(filename);
    PrintStream printStream = new PrintStream(fileOutputStream);
    printStream.println(schema.toString(true));
    printStream.close();
  }

  @Override
  public String getName() {
    return "idl2schemata";
  }

  @Override
  public String getShortDescription() {
    return "Extract JSON schemata of the types from an Avro IDL file";
  }
}
