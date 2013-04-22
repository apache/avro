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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** Static utility methods for tools. */
class Util {
  /**
   * Returns stdin if filename is "-", else opens the File in the owning filesystem
   * and returns an InputStream for it.
   * Relative paths will be opened in the default filesystem. 
   * @param filename The filename to be opened
   * @throws IOException 
   */
  static BufferedInputStream fileOrStdin(String filename, InputStream stdin) 
      throws IOException {
    return new BufferedInputStream(filename.equals("-")
        ? stdin
        : openFromFS(filename));  
  }
  
  /**
   * Returns stdout if filename is "-", else opens the file from the owning filesystem
   * and returns an OutputStream for it.
   * Relative paths will be opened in the default filesystem.  
   * @param filename The filename to be opened
   * @throws IOException 
   */
  static BufferedOutputStream fileOrStdout(String filename, OutputStream stdout) 
      throws IOException {
    return new BufferedOutputStream(filename.equals("-")
        ? stdout
        : createFromFS(filename));
  }
  
  /**
   * Returns an InputStream for the file using the owning filesystem,
   * or the default if none is given.
   * @param filename The filename to be opened
   * @throws IOException 
   */
  static InputStream openFromFS(String filename) 
      throws IOException {
    Path p = new Path(filename);
    return p.getFileSystem(new Configuration()).open(p);
  }
  
  /**
   * Returns a seekable FsInput using the owning filesystem, 
   * or the default if none is given.
   * @param filename The filename to be opened
   * @throws IOException 
   */
  static FsInput openSeekableFromFS(String filename) 
      throws IOException {       
    return new FsInput(new Path(filename), new Configuration());
  }
  
  /**
   * Opens the file for writing in the owning filesystem,
   * or the default if none is given.
   * @param filename The filename to be opened.
   * @return An OutputStream to the specified file.
   * @throws IOException
   */
  static OutputStream createFromFS(String filename) 
      throws IOException {
    Path p = new Path(filename);
    return new BufferedOutputStream(p.getFileSystem(new Configuration()).create(p));
  }
  
  /**
   * Closes the inputstream created from {@link Util.fileOrStdin} 
   * unless it is System.in.
   * @param in The inputstream to be closed.
   */
  static void close(InputStream in) {
    if (!System.in.equals(in)) {
      try {
        in.close();
      } catch (IOException e) {
        System.err.println("could not close InputStream " + in.toString());
      }
    }
  }
  
  /**
   * Closes the outputstream created from {@link Util.fileOrStdout}
   * unless it is System.out.
   * @param out The outputStream to be closed.
   */
  static void close(OutputStream out) {
    if (!System.out.equals(out)) {
      try {
        out.close();
      } catch (IOException e) {
        System.err.println("could not close OutputStream " + out.toString());
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
