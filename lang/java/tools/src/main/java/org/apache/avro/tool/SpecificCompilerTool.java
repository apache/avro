/*
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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.compiler.specific.SpecificCompiler;

/**
 * A Tool for compiling avro protocols or schemas to Java classes using the Avro
 * SpecificCompiler.
 */

public class SpecificCompilerTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() < 3) {
      System.err
          .println("Usage: [-encoding <outputencoding>] [-string] [-bigDecimal] (schema|protocol) input... outputdir");
      System.err
          .println(" input - input files or directories");
      System.err
          .println(" outputdir - directory to write generated java");
      System.err.println(" -encoding <outputencoding> - set the encoding of " +
          "output file(s)");
      System.err.println(" -string - use java.lang.String instead of Utf8");
      System.err.println(" -bigDecimal - use java.math.BigDecimal for " +
          "decimal type instead of java.nio.ByteBuffer");
      return 1;
    }

    StringType stringType = StringType.CharSequence;
    boolean useLogicalDecimal = false;

    int arg = 0;

    String encoding = null;
    if ("-encoding".equals(args.get(arg))) {
      arg++;
      encoding = args.get(arg);
      arg++;
    }

    if ("-string".equals(args.get(arg))) {
      stringType = StringType.String;
      arg++;
    }

    if ("-bigDecimal".equalsIgnoreCase(args.get(arg))) {
      useLogicalDecimal = true;
      arg++;
    }

    String method = args.get(arg);
    List<File> inputs = new ArrayList<>();
    File output = new File(args.get(args.size() - 1));

    for (int i = arg+1; i < args.size() - 1; i++) {
      inputs.add(new File(args.get(i)));
    }

    if ("schema".equals(method)) {
      Schema.Parser parser = new Schema.Parser();
      for (File src : determineInputs(inputs, SCHEMA_FILTER)) {
        Schema schema = parser.parse(src);
        SpecificCompiler compiler = new SpecificCompiler(schema);
        executeCompiler(compiler, encoding, stringType, useLogicalDecimal, src, output);
      }
    } else if ("protocol".equals(method)) {
      for (File src : determineInputs(inputs, PROTOCOL_FILTER)) {
        Protocol protocol = Protocol.parse(src);
        SpecificCompiler compiler = new SpecificCompiler(protocol);
        executeCompiler(compiler, encoding, stringType, useLogicalDecimal, src, output);
      }
    } else {
      System.err.println("Expected \"schema\" or \"protocol\".");
      return 1;
    }
    return 0;
  }

  private void executeCompiler(SpecificCompiler compiler,
                               String encoding,
                               StringType stringType,
                               boolean enableDecimalLogicalType,
                               File src,
                               File output) throws IOException {
    compiler.setStringType(stringType);
    compiler.setEnableDecimalLogicalType(enableDecimalLogicalType);
    if (encoding != null) {
      compiler.setOutputCharacterEncoding(encoding);
    }
    compiler.compileToDestination(src, output);
  }

  @Override
  public String getName() {
    return "compile";
  }

  @Override
  public String getShortDescription() {
    return "Generates Java code for the given schema.";
  }

  /**
   * For a List of files or directories, returns a File[] containing each file
   * passed as well as each file with a matching extension found in the directory.
   *
   * @param inputs List of File objects that are files or directories
   * @param filter File extension filter to match on when fetching files from a directory
   * @return Unique array of files
   */
  private static File[] determineInputs(List<File> inputs, FilenameFilter filter) {
    Set<File> fileSet = new LinkedHashSet<>(); // preserve order and uniqueness

    for (File file : inputs) {
      // if directory, look at contents to see what files match extension
      if (file.isDirectory()) {
        for (File f : file.listFiles(filter)) {
          fileSet.add(f);
        }
      }
      // otherwise, just add the file.
      else {
        fileSet.add(file);
      }
    }

    if (fileSet.size() > 0) {
      System.err.println("Input files to compile:");
      for (File file : fileSet) {
        System.err.println("  " + file);
      }
    }
    else {
      System.err.println("No input files found.");
    }

    return fileSet.toArray((new File[fileSet.size()]));
  }

  private static final FileExtensionFilter SCHEMA_FILTER =
    new FileExtensionFilter("avsc");
  private static final FileExtensionFilter PROTOCOL_FILTER =
    new FileExtensionFilter("avpr");

  private static class FileExtensionFilter implements FilenameFilter {
    private String extension;

    private FileExtensionFilter(String extension) {
      this.extension = extension;
    }

    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(this.extension);
    }
  }
}
