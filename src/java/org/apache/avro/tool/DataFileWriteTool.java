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

import java.io.EOFException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.JsonDecoder;

/** Reads new-line delimited JSON records and writers an Avro data file. */
public class DataFileWriteTool implements Tool {

  @Override
  public String getName() {
    return "fromjson";
  }

  @Override
  public String getShortDescription() {
    return "Reads JSON records and writes an Avro data file.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 2) {
      err.println("Expected 2 args: schema input_file");
      return 1;
    }
    
    Schema schema = Schema.parse(args.get(0));
    DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    
    InputStream input = Util.fileOrStdin(args.get(1), stdin);
    try {
      DataInputStream din = new DataInputStream(input);
      DataFileWriter<Object> writer =
        new DataFileWriter<Object>(schema, out,
                                   new GenericDatumWriter<Object>());
      Decoder decoder = new JsonDecoder(schema, din);
      Object datum;
      while (true) {
        try {
          datum = reader.read(null, decoder);
        } catch (EOFException e) {
          break;
        }
        writer.append(datum);
      }
      writer.close();
    } finally {
      if (input != stdin) {
        input.close();
      }
    }
    return 0;
  }
}
