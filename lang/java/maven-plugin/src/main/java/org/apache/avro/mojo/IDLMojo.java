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

import org.apache.avro.Protocol;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.idl.IdlFile;
import org.apache.avro.idl.IdlReader;
import org.apache.maven.artifact.DependencyResolutionRequiredException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
        Path sourceFilePath = sourceDirectory.toPath().resolve(filename);
        IdlFile idlFile = parser.parse(sourceFilePath);
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
        setCompilerProperties(compiler);
        for (String customConversion : customConversions) {
          compiler.addCustomConversion(projPathLoader.loadClass(customConversion));
        }
        compiler.compileToDestination(sourceFilePath.toFile(), outputDirectory);
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
}
