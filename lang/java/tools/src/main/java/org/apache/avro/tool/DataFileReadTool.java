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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

/** Reads a data file and dumps to JSON */
public class DataFileReadTool implements Tool {

  @Override
  public String getName() {
    return "tojson";
  }

  @Override
  public String getShortDescription() {
    return "Dumps an Avro data file as JSON, record per line or pretty.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    OptionParser optionParser = new OptionParser();
    OptionSpec<Void> prettyOption = optionParser
        .accepts("pretty", "Turns on pretty printing.");

    OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
    Boolean pretty = optionSet.has(prettyOption);
    List<String> nargs = optionSet.nonOptionArguments();

    if (nargs.size() != 1) {
      printHelp(err);
      err.println();
      optionParser.printHelpOn(err);
      return 1;
    }

    BufferedInputStream inStream = Util.fileOrStdin(nargs.get(0), stdin);

    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    DataFileStream<Object> streamReader = new DataFileStream<Object>(inStream, reader);
    try {
      Schema schema = streamReader.getSchema();
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out, pretty);
      for (Object datum : streamReader)
        writer.write(datum, encoder);
      encoder.flush();
      out.println();
      out.flush();
    } finally {
      streamReader.close();
    }
    return 0;
  }

  private void printHelp(PrintStream ps) {
    ps.println("tojson --pretty input-file");
    ps.println();
    ps.println(getShortDescription());
    ps.println("A dash ('-') can be given as an input file to use stdin");
  }
}
