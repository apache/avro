/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.mojo;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generate Java classes from Avro schema files (.avsc)
 *
 * @goal schema
 * @phase generate-sources
 * @requiresDependencyResolution runtime+test
 * @threadSafe
 */
public class SchemaMojo extends AbstractAvroMojo {
  /**
   * A parser used to parse all schema files. Using a common parser will
   * facilitate the import of external schemas.
   */
  private Schema.Parser schemaParser = new Schema.Parser();

  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern <code>**&#47;*.avsc</code>
   * is used to select grammar files.
   *
   * @parameter
   */
  private String[] includes = new String[] { "**/*.avsc" };

  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern <code>**&#47;*.avsc</code>
   * is used to select grammar files.
   *
   * @parameter
   */
  private String[] testIncludes = new String[] { "**/*.avsc" };

  @Override
  protected void doCompile(String[] fileNames, File sourceDirectory, File outputDirectory)
      throws MojoExecutionException {
    final List<File> sourceFiles = Arrays.stream(fileNames)
        .map((String filename) -> new File(sourceDirectory, filename)).collect(Collectors.toList());
    final File sourceFileForModificationDetection = sourceFiles.stream().filter(file -> file.lastModified() > 0)
        .max(Comparator.comparing(File::lastModified)).orElse(null);
    final List<Schema> schemas;

    try {
      // This is necessary to maintain backward-compatibility. If there are
      // no imported files then isolate the schemas from each other, otherwise
      // allow them to share a single schema so reuse and sharing of schema
      // is possible.
      if (imports == null) {
        schemas = new Schema.Parser().parse(sourceFiles);
      } else {
        schemas = schemaParser.parse(sourceFiles);
      }

      doCompile(sourceFileForModificationDetection, schemas, outputDirectory);
    } catch (IOException | SchemaParseException ex) {
      throw new MojoExecutionException("Error compiling a file in " + sourceDirectory + " to " + outputDirectory, ex);
    }
  }

  @Override
  protected String[] getIncludes() {
    return includes;
  }

  @Override
  protected String[] getTestIncludes() {
    return testIncludes;
  }
}
