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
import org.apache.avro.genavro.GenAvro;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * Tool implementation for generating Avro JSON schemata from
 * genavro format files.
 */
public class GenAvroTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
                  List<String> args) throws Exception {

    InputStream parseIn = in;
    PrintStream parseOut = out;

    if (args.size() > 2 ||
        (args.size() == 1 && (args.get(0).equals("--help") ||
                              args.get(0).equals("-help")))) {
      err.println("Usage: GenAvroTool [in] [out]");
      err.println("");
      err.println("If an output path is not specified, outputs to stdout.");
      err.println("If no input or output is specified, takes input from");
      err.println("stdin and outputs to stdin.");
      err.println("The special path \"-\" may also be specified to refer to");
      err.println("stdin and stdout.");
      return -1;
    }

    if (args.size() >= 1 && ! "-".equals(args.get(0))) {
      parseIn = new FileInputStream(args.get(0));
    }
    if (args.size() == 2 && ! "-".equals(args.get(1))) {
      parseOut = new PrintStream(new FileOutputStream(args.get(1)));
    }


    GenAvro parser = new GenAvro(parseIn);
    Protocol p = parser.CompilationUnit();
    parseOut.print(p.toString(true));
    return 0;
  }

  @Override
  public String getName() {
    return "genavro";
  }

  @Override
  public String getShortDescription() {
    return "Generates a JSON schema from a GenAvro file";
  }
}
