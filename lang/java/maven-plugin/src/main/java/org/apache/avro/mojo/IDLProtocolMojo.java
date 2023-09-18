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
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.avro.idl.IdlFile;
import org.apache.avro.idl.IdlReader;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Generate Java classes and interfaces from AvroIDL files (.avdl)
 *
 * @goal idl-protocol
 * @requiresDependencyResolution runtime
 * @phase generate-sources
 * @threadSafe
 */
public class IDLProtocolMojo extends AbstractAvroMojo {
  /**
   * Use the classic JavaCC parser for <code>.avdl</code> files. If
   * <code>false</code> (the default), use the new ANTLR parser instead.
   *
   * @parameter
   */
  private boolean useJavaCC = false;

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
    File sourceFile = new File(sourceDirectory, filename);

    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    URL[] extraClasspath = new URL[] { sourceDirectory.toURI().toURL() };
    ClassLoader classLoader = new URLClassLoader(extraClasspath, contextClassLoader);

    Protocol protocol;
    if (useJavaCC) {
      try (Idl idl = new Idl(sourceFile, classLoader)) {
        final Protocol p = idl.CompilationUnit();
        String json = p.toString(true);
        protocol = Protocol.parse(json);
      } catch (ParseException e) {
        throw new IOException(e);
      }
    } else {
      try {
        Thread.currentThread().setContextClassLoader(classLoader);

        IdlReader parser = new IdlReader();
        IdlFile idlFile = parser.parse(sourceFile.toPath());
        for (String warning : idlFile.getWarnings()) {
          getLog().warn(warning);
        }
        protocol = idlFile.getProtocol();
      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    }

    doCompile(sourceFile, protocol, outputDirectory);
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
