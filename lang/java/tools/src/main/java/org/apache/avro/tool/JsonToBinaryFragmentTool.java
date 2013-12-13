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
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;

/** Tool to convert JSON data into the binary form. */
public class JsonToBinaryFragmentTool implements Tool {
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() != 2) {
      err.println("Expected 2 arguments: schema json_data_file");
      err.println("Use '-' as json_data_file for stdin.");
      return 1;
    }
    Schema schema = new Schema.Parser().parse(args.get(0));
    InputStream input = Util.fileOrStdin(args.get(1), stdin);
    try {
    GenericDatumReader<Object> reader = 
        new GenericDatumReader<Object>(schema);
    
    JsonDecoder jsonDecoder = 
      DecoderFactory.get().jsonDecoder(schema, input);
    GenericDatumWriter<Object> writer = 
        new GenericDatumWriter<Object>(schema);
    Encoder e = EncoderFactory.get().binaryEncoder(out, null);
    Object datum = null;
    try {
      while(true) {
        datum = reader.read(datum, jsonDecoder);
        writer.write(datum, e);
        e.flush();
      }
    } catch (EOFException eofException) {}
    } finally {
      Util.close(input);
    }
    return 0;
  }

  @Override
  public String getName() {
    return "jsontofrag";
  }

  @Override
  public String getShortDescription() {
    return "Renders a JSON-encoded Avro datum as binary.";
  }
}
