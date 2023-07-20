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

import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData.StringType;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.MojoExecutionException;

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

  /**
   * generated record schema classes will extend this class.
   */
  private String recordSpecificClass = "org.apache.avro.specific.SpecificRecordBase";

  /**
   * generated error schema classes will extend this class.
   */
  private String errorSpecificClass = "org.apache.avro.specific.SpecificExceptionBase";

  @Override
  protected void doCompile(String[] filesName, File sourceDirectory, File outputDirectory)
      throws MojoExecutionException {
    final List<File> sourceFiles = Arrays.stream(filesName)
        .map((String filename) -> new File(sourceDirectory, filename)).collect(Collectors.toList());
    final List<Schema> schemas;

    // This is necessary to maintain backward-compatibility. If there are
    // no imported files then isolate the schemas from each other, otherwise
    // allow them to share a single schema so reuse and sharing of schema
    // is possible.
    try {
      if (imports == null) {
        schemas = new Schema.Parser().parse(sourceFiles);
      } else {
        schemas = schemaParser.parse(sourceFiles);
      }
    } catch (IOException | SchemaParseException ex) {
      throw new MojoExecutionException("Error compiling one file of " + sourceDirectory + " to " + outputDirectory, ex);
    }

    final SpecificCompiler compiler = new SpecificCompiler(schemas);
    compiler.setTemplateDir(templateDirectory);
    compiler.setStringType(StringType.valueOf(stringType));
    compiler.setFieldVisibility(getFieldVisibility());
    compiler.setCreateOptionalGetters(createOptionalGetters);
    compiler.setGettersReturnOptional(gettersReturnOptional);
    compiler.setOptionalGettersForNullableFieldsOnly(optionalGettersForNullableFieldsOnly);
    compiler.setCreateSetters(createSetters);
    compiler.setEnableDecimalLogicalType(enableDecimalLogicalType);
    try {
      final URLClassLoader classLoader = createClassLoader();
      for (String customConversion : customConversions) {
        compiler.addCustomConversion(classLoader.loadClass(customConversion));
      }
    } catch (ClassNotFoundException | DependencyResolutionRequiredException | MalformedURLException e) {
      throw new MojoExecutionException("Compilation error: Can't add custom conversion", e);
    }
    compiler.setOutputCharacterEncoding(project.getProperties().getProperty("project.build.sourceEncoding"));
    compiler.setAdditionalVelocityTools(instantiateAdditionalVelocityTools());
    compiler.setRecordSpecificClass(this.recordSpecificClass);
    compiler.setErrorSpecificClass(this.errorSpecificClass);
    for (File src : sourceFiles) {
      try {
        compiler.compileToDestination(src, outputDirectory);
      } catch (IOException ex) {
        throw new MojoExecutionException("Compilation error with file " + src + " to " + outputDirectory, ex);
      }
    }
  }

  @Override
  protected void doCompile(final String filename, final File sourceDirectory, final File outputDirectory)
      throws IOException {
    // Not call.
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
