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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Tool to concatenate avro files with the same schema and non-reserved
 * metatdata.
 */
public class ConcatTool implements Tool {
  static final String APPEND_ARG = "-appendToFirst";

  /**
   * @return 0 for success, 1 if the schemas of the input files differ, 2 if
   *         the non-reserved input metadata differs, 3 if the input files are
   *         encoded with more than one codec.
   */
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {

    if(args.isEmpty()) {
      printHelp(out);
      return 0;
    }

    String appendedFilename = null;
    OutputStream output = out;
    Schema schema = null;
    Map<String, byte[]> metadata = new TreeMap<String, byte[]>();
    String inputCodec = null;
    DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
            new GenericDatumWriter<GenericRecord>());

    if (args.size() > 1) {
      if(args.contains(APPEND_ARG)) {
        args = new ArrayList<String>(args);
        args.remove(APPEND_ARG);
        appendedFilename = args.get(0);
        output = Util.fileOrStdout(appendedFilename, out, true);
        args.remove(0);

        // read state so that when the first file to be appended is read below its schema/meta gets compared to the appended file
        DataFileStream<GenericRecord> outputReader = new DataFileStream<GenericRecord>(
                Util.fileOrStdin(appendedFilename, in), new GenericDatumReader<GenericRecord>());
        schema = outputReader.getSchema();
        extractMetadata(outputReader, metadata, null);
        inputCodec = getInputCodec(outputReader);
        outputReader.close();

        writer.appendTo(new FsInput(new Path(appendedFilename), new Configuration()), output);
      } else {
        output = Util.fileOrStdout(args.get(args.size() - 1), out, false);
        args = args.subList(0, args.size() - 1);
      }
    }

    for (String inFile : args) {
      InputStream input = Util.fileOrStdin(inFile, in);
      DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(
        input, new GenericDatumReader<GenericRecord>());

      if (schema == null) {
        // this is the first file - set up the writer, and store the
        // Schema & metadata we'll use.
        schema = reader.getSchema();
        extractMetadata(reader, metadata, writer);
        inputCodec = getInputCodec(reader);
        writer.setCodec(CodecFactory.fromString(inputCodec));
        writer.create(schema, output);
      } else {
        // check that we're appending to the same schema & metadata.
        if (!schema.equals(reader.getSchema())) {
          err.println("input files have different schemas");
          reader.close();
          return 1;
        }
        for (String key : reader.getMetaKeys()) {
          if (!DataFileWriter.isReservedMeta(key)) {
            byte[] metadatum = reader.getMeta(key);
            byte[] writersMetadatum = metadata.get(key);
            if(!Arrays.equals(metadatum, writersMetadatum)) {
              err.println("input files have different non-reserved metadata");
              reader.close();
              return 2;
            }
          }
        }
        String thisCodec = reader.getMetaString(DataFileConstants.CODEC);
        if(thisCodec == null) {
          thisCodec = DataFileConstants.NULL_CODEC;
        }
        if (!inputCodec.equals(thisCodec)) {
          err.println("input files have different codecs");
          reader.close();
          return 3;
        }
      }

      writer.appendAllFrom(reader, /*recompress*/ false);
      reader.close();
    }

    writer.close();
    return 0;
  }

  private String getInputCodec(DataFileStream<GenericRecord> reader) {
    String inputCodec;
    inputCodec = reader.getMetaString(DataFileConstants.CODEC);
    if (inputCodec == null) {
      inputCodec = DataFileConstants.NULL_CODEC;
    }
    return inputCodec;
  }

  private void extractMetadata(DataFileStream<GenericRecord> reader, Map<String, byte[]> metadata, DataFileWriter<GenericRecord> writer) {
    for (String key : reader.getMetaKeys()) {
      if (!DataFileWriter.isReservedMeta(key)) {
        byte[] metadatum = reader.getMeta(key);
        metadata.put(key, metadatum);
        if(writer != null) writer.setMeta(key, metadatum);
      }
    }
  }

  private void printHelp(PrintStream out) {
    out.println("concat [-appendToFirst] [input-file...] [output-file]");
    out.println();
    out.println("Concatenates one or more input files into an output file");
    out.println("by appending the input blocks without decoding them. The input");
    out.println("files must have the same schema, metadata and codec. If they");
    out.println("do not the tool will return the following error codes:");
    out.println("  1 if the schemas don't match");
    out.println("  2 if the metadata doesn't match");
    out.println("  3 if the codecs don't match");
    out.println("If no input files are given stdin will be used. The tool");
    out.println("0 on success. A dash ('-') can be given as an input file");
    out.println("to use stdin, and as an output file to use stdout.");
    out.println("Use the -appendToFirst flag to append the 2nd-Nth files to");
    out.println("the first. Otherwise a new output file is created.");
  }

@Override
  public String getName() {
    return "concat";
  }

  @Override
  public String getShortDescription() {
    return "Concatenates avro files without re-compressing.";
  }
}
