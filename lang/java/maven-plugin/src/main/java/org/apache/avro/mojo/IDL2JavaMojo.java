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

import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.idl.IdlFile;

import java.io.File;
import java.io.IOException;

/**
 * Generate Java classes and interfaces (.java) from AvroIDL files (.avdl)
 *
 * @goal idl2java
 * @requiresDependencyResolution runtime
 * @phase generate-sources
 * @threadSafe
 */
public class IDL2JavaMojo extends AbstractIDLMojo {
  @Override
  protected void doCompile(String filename, File sourceDirectory, File outputDirectory) throws IOException {
    try {
      final IdlFile idlFile = parseIdlFile(filename, sourceDirectory);
      final SpecificCompiler compiler;
      if (idlFile.getProtocol() != null) {
        compiler = new SpecificCompiler(idlFile.getProtocol());
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
        compiler.addCustomConversion(Thread.currentThread().getContextClassLoader().loadClass(customConversion));
      }
      compiler.setOutputCharacterEncoding(project.getProperties().getProperty("project.build.sourceEncoding"));
      compiler.compileToDestination(null, outputDirectory);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}
