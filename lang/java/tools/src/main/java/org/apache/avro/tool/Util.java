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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Static utility methods for tools. */
class Util {
  /**
   * Returns stdin if filename is "-", else opens the local or HDFS file
   * and returns an InputStream for it.
   * @throws IOException 
   */
  static BufferedInputStream fileOrStdin(String filename, InputStream stdin) 
      throws IOException {
    if (filename.equals("-")) {
      return new BufferedInputStream(stdin);
    } 
    else {
      String[] parts = filename.split(":");
      if (parts.length > 1 && parts[0].equals("hdfs")) {
        FileSystem fs = FileSystem.get(
            URI.create(filename), new Configuration());
        return new BufferedInputStream(fs.open(new Path(filename)));
      } else {
        return new BufferedInputStream(new FileInputStream(new File(filename)));
      }
    }
  }
  
  /**
   * Returns stdout if filename is "-", else opens the local or HDFS file
   * and returns an OutputStream for it.
   * @throws IOException 
   */
  static BufferedOutputStream fileOrStdout(String filename, OutputStream stdout) 
  throws IOException {
    if (filename.equals("-")) {
      return new BufferedOutputStream(stdout);
    } 
    else {
      String[] parts = filename.split(":");
      if (parts.length > 1 && parts[0].equals("hdfs")) {
        FileSystem fs = FileSystem.get(
          URI.create(filename), new Configuration());
        return new BufferedOutputStream(fs.create(new Path(filename)));
      } else {
        return new BufferedOutputStream(
            new FileOutputStream(new File(filename)));
      }
    }
  }
  
  /** 
   * Converts a String JSON object into a generic datum.
   * 
   * This is inefficient (creates extra objects), so should be used 
   * sparingly.
   */
  static Object jsonToGenericDatum(Schema schema, String jsonData)
      throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Object datum = reader.read(null,
        DecoderFactory.get().jsonDecoder(schema, jsonData));
    return datum;
  }

  /** Reads and returns the first datum in a data file. */
  static Object datumFromFile(Schema schema, String file) throws IOException {
    DataFileReader<Object> in =
      new DataFileReader<Object>(new File(file),
                                 new GenericDatumReader<Object>(schema));
    try {
      return in.next();
    } finally {
      in.close();
    }
  }

}
