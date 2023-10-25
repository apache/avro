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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;

import org.apache.avro.idl.IdlFile;
import org.apache.avro.idl.IdlReader;
import org.apache.maven.artifact.DependencyResolutionRequiredException;

import static org.apache.avro.compiler.specific.SpecificCompiler.mangle;
import static org.apache.avro.compiler.specific.SpecificCompiler.mangleTypeIdentifier;

/**
 * Generate Java classes and interfaces from AvroIDL files (.avdl)
 *
 * @goal idl
 * @requiresDependencyResolution runtime
 * @phase generate-sources
 * @threadSafe
 */
public class IDLMojo extends AbstractAvroMojo {
  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern <code>**&#47;*.avdl</code>
   * is used to select IDL files.
   *
   * @parameter
   */
  private String[] includes = new String[] { "**/*.avdl" };

  /**
   * A set of Ant-like inclusion patterns used to select files from the source
   * directory for processing. By default, the pattern <code>**&#47;*.avdl</code>
   * is used to select IDL files.
   *
   * @parameter
   */
  private String[] testIncludes = new String[] { "**/*.avdl" };

  /**
   * The generateJava parameter determines whether to generate Java class files
   * (*.java)
   *
   * @parameter property="generateJava" default-value=true
   */
  protected boolean generateJava = true;

  /**
   * The generateProtocol parameter determines whether to generate Avro Protocol
   * files (*.avpr)
   *
   * @parameter property="generateProtocol" default-value=false
   */
  protected boolean generateProtocol = false;

  /**
   * The generateSchema parameter determines whether to generate Avro Protocol
   * files (*.avsc)
   *
   * @parameter property="generateSchema" default-value=false
   */
  protected boolean generateSchema = false;

  @Override
  protected void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException {
    try {
      @SuppressWarnings("rawtypes")
      List runtimeClasspathElements = project.getRuntimeClasspathElements();

      List<URL> runtimeUrls = new ArrayList<>();

      // Add the source directory of avro files to the classpath so that
      // imports can refer to other idl files as classpath resources
      runtimeUrls.add(sourceDirectory.toURI().toURL());

      // If runtimeClasspathElements is not empty values add its values to Idl path.
      if (runtimeClasspathElements != null && !runtimeClasspathElements.isEmpty()) {
        for (Object runtimeClasspathElement : runtimeClasspathElements) {
          String element = (String) runtimeClasspathElement;
          runtimeUrls.add(new File(element).toURI().toURL());
        }
      }

      final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      URLClassLoader projPathLoader = new URLClassLoader(runtimeUrls.toArray(new URL[0]), contextClassLoader);
      Thread.currentThread().setContextClassLoader(projPathLoader);
      try {
        IdlReader parser = new IdlReader();
        IdlFile idlFile = parser.parse(sourceDirectory.toPath().resolve(filename));
        for (String warning : idlFile.getWarnings()) {
          getLog().warn(warning);
        }
        final SpecificCompiler compiler;
        final Protocol protocol = idlFile.getProtocol();
        if (protocol != null) {
          compiler = new SpecificCompiler(protocol);
        } else {
          compiler = new SpecificCompiler(idlFile.getNamedSchemas().values());
        }
        compiler.setStringType(GenericData.StringType.valueOf(stringType));
        compiler.setTemplateDir(templateDirectory);
        compiler.setFieldVisibility(getFieldVisibility());
        compiler.setCreateOptionalGetters(createOptionalGetters);
        compiler.setGettersReturnOptional(gettersReturnOptional);
        compiler.setOptionalGettersForNullableFieldsOnly(optionalGettersForNullableFieldsOnly);
        compiler.setCreateSetters(createSetters);
        compiler.setAdditionalVelocityTools(instantiateAdditionalVelocityTools());
        compiler.setEnableDecimalLogicalType(enableDecimalLogicalType);
        for (String customConversion : customConversions) {
          compiler.addCustomConversion(projPathLoader.loadClass(customConversion));
        }
        compiler.setOutputCharacterEncoding(project.getProperties().getProperty("project.build.sourceEncoding"));

        if (generateJava) {
          compiler.compileToDestination(null, outputDirectory);
        }
        if (generateProtocol && protocol != null) {
          printProtocol(protocol, outputDirectory, true);
        }
        if (generateSchema) {
          Collection<Schema> schemas;
          if (protocol != null) {
            schemas = protocol.getTypes();
          } else {
            schemas = idlFile.getNamedSchemas().values();
          }

          for (Schema schema : schemas) {
            printSchema(schema, outputDirectory, true);
          }
        }
      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    } catch (ClassNotFoundException | DependencyResolutionRequiredException e) {
      throw new IOException(e);
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

  private String makePath(String name, String space, String suffix) {
    if (space == null || space.isEmpty()) {
      return name + suffix;
    } else {
      return space.replace('.', File.separatorChar) + File.separatorChar + name + suffix;
    }
  }

  private void printProtocol(Protocol protocol, File outputDirectory, boolean pretty) throws IOException {
    String name = mangleTypeIdentifier(protocol.getName());
    String protocolPath = makePath(name, mangle(protocol.getNamespace()), ".avpr");
    String filePath = outputDirectory.getAbsolutePath() + "/" + protocolPath;
    printJson(protocol.toString(pretty), filePath);
  }

  private void printSchema(Schema schema, File outputDirectory, boolean pretty) throws IOException {
    String name = mangleTypeIdentifier(schema.getName());
    String schemaPath = makePath(name, mangle(schema.getNamespace()), ".avsc");
    String filePath = outputDirectory.getAbsolutePath() + "/" + schemaPath;
    printJson(schema.toString(pretty), filePath);
  }

  private void printJson(String jsonString, String filePath) throws IOException {
    new File(filePath).getParentFile().mkdirs();
    FileOutputStream fileOutputStream = new FileOutputStream(filePath);
    PrintStream printStream = new PrintStream(fileOutputStream);
    printStream.println(jsonString);
    printStream.close();
  }
}
