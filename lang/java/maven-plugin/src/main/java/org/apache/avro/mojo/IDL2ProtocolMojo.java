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
import org.apache.avro.idl.IdlFile;

import java.io.File;
import java.io.IOException;

import static org.apache.avro.compiler.specific.SpecificCompiler.mangle;
import static org.apache.avro.compiler.specific.SpecificCompiler.mangleTypeIdentifier;

/**
 * Generate Java classes and interfaces from AvroIDL files (.avdl)
 *
 * @goal idl2protocol
 * @requiresDependencyResolution runtime
 * @phase generate-sources
 * @threadSafe
 */
public class IDL2ProtocolMojo extends AbstractIDLMojo {
  @Override
  protected void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException {
    final IdlFile idlFile = parseIdlFile(filename, sourceDirectory);
    final Protocol protocol = idlFile.getProtocol();
    if (protocol != null) {
      printProtocol(protocol, outputDirectory, true);
    }
  }

  private void printProtocol(Protocol protocol, File outputDirectory, boolean pretty) throws IOException {
    String name = mangleTypeIdentifier(protocol.getName());
    String protocolPath = makePath(name, mangle(protocol.getNamespace()), ".avpr");
    String filePath = outputDirectory.getAbsolutePath() + "/" + protocolPath;
    printJson(protocol.toString(pretty), filePath);
  }
}
