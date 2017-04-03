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

import org.apache.avro.Protocol;
import org.apache.avro.compiler.idl.Idl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;

/**
 * Tool implementation for generating Avro JSON schemata from
 * idl format files.
 */
public class IdlTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
                  List<String> args) throws Exception {

    PrintStream parseOut = out;

    boolean incorrectArgsOrHelpRequested = false;
    List<File> includeDirs = new ArrayList<File>();
    String fileIn = null;
    String fileOut = null;

    for (int arg = 0; arg < args.size(); ++arg) {
        if ("--help".equals(args.get(arg))) {
            incorrectArgsOrHelpRequested = true;
            break;
        } else if ("--include".equals(args.get(arg))) {
            arg += 1;
            String fName = arg < args.size() ? args.get(arg) : "";
            File f = new File(fName);
            if (f.exists() && f.isDirectory()) {
                includeDirs.add(f);
            } else {
                err.println("Specified --include path '" + fName + "' does not exists or is not a directory!");
                incorrectArgsOrHelpRequested = true;
                break;
            }
        } else /*positional argument*/ {
            if (fileIn == null) {
                fileIn = args.get(arg);
                if (!"-".equals(fileIn)) {
                    File f = new File(fileIn);
                    if (!f.exists() || f.isDirectory()) {
                        err.println("Specified input path is not an existing file or is a directory!");
                        incorrectArgsOrHelpRequested = true;
                        break;
                    }
                }
            } else if (fileOut == null) {
                fileOut = args.get(arg);
            } else {
                incorrectArgsOrHelpRequested = true;
                break;
            }
        }
    }


    if (incorrectArgsOrHelpRequested) {
      err.println("Usage: idl [--include includeDir ...] [in] [out]");
      err.println("");
      err.println("If an output path is not specified, outputs to stdout.");
      err.println("If no input or output is specified, takes input from");
      err.println("stdin and outputs to stdin.");
      err.println("The special path \"-\" may also be specified to refer to");
      err.println("stdin and stdout.");
      err.println("If any --include options are specified. The specified");
      err.println("include dirs are also used to resolve a import statement");
      return -1;
    }

    Idl parser;
    if (fileIn != null && ! "-".equals(fileIn)) {
      parser = new Idl(new File(fileIn));
    } else {
      parser = new Idl(in);
    }

    if (fileOut != null && ! "-".equals(fileOut)) {
      parseOut = new PrintStream(new FileOutputStream(fileOut));
    }

    for (File includeDir : includeDirs) {
        parser.addIncludeDir(includeDir);
    }

    Protocol p = parser.CompilationUnit();
    parseOut.print(p.toString(true));
    return 0;
  }

  @Override
  public String getName() {
    return "idl";
  }

  @Override
  public String getShortDescription() {
    return "Generates a JSON schema from an Avro IDL file";
  }
}
