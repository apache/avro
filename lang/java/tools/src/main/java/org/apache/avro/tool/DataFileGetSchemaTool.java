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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

/** Reads a data file to get its schema. */
public class DataFileGetSchemaTool implements Tool {

  @Override
  public String getName() {
    return "getschema";
  }

  @Override
  public String getShortDescription() {
    return "Prints out schema of an Avro data file.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 1) {
      err.println("Expected 1 argument: input_file or -");
      return 1;
    }
    BufferedInputStream inStream = Util.fileOrStdin(args.get(0), stdin);

    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    DataFileStream<Object> streamReader = new DataFileStream<Object>(inStream, reader);
    out.println(streamReader.getSchema().toString(true));
    return 0;
  }
}
