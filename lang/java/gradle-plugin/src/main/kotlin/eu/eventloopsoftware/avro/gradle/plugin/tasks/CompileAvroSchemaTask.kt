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
package eu.eventloopsoftware.avro.gradle.plugin.tasks

import java.io.File
import java.io.IOException
import org.apache.avro.Protocol
import org.apache.avro.SchemaParseException
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.TaskAction

abstract class CompileAvroSchemaTask : AbstractCompileTask() {

  @get:InputFiles @get:SkipWhenEmpty abstract val schemaFiles: ConfigurableFileCollection

  @get:InputFiles @get:SkipWhenEmpty abstract val protocolFiles: ConfigurableFileCollection

  @TaskAction
  fun compileSchema() {
    logger.info("Generating Java files from ${schemaFiles.files.size} Avro schemas...")

    compileSchemas(schemaFiles, outputDirectory.get().asFile)

    logger.info("Done generating Java files from Avro schemas...")
  }

  private fun compileSchemas(schemaFileTree: ConfigurableFileCollection, outputDirectory: File) {
    val sourceFileForModificationDetection: File? =
        schemaFileTree.asFileTree.files.filter { file: File -> file.lastModified() > 0 }.maxBy { it.lastModified() }

    // Need to register custom logical type factories before schema compilation.
    try {
      loadLogicalTypesFactories()
    } catch (e: IOException) {
      throw RuntimeException("Error while loading logical types factories ", e)
    }

    try {
      val parser = SchemaParser()
      for (sourceFile in schemaFileTree.files) {
        parser.parse(sourceFile)
      }
      val schemas = parser.parsedNamedSchemas
      doCompile(sourceFileForModificationDetection, SpecificCompiler(schemas), outputDirectory)

      for (sourceFile in protocolFiles.files) {
        val protocol = Protocol.parse(sourceFile)
        doCompile(sourceFile, protocol, outputDirectory)
      }
    } catch (ex: IOException) {
      throw RuntimeException(
          "IO ex: Error compiling a file in " + schemaFileTree.asPath + " to " + outputDirectory,
          ex,
      )
    } catch (ex: SchemaParseException) {
      throw RuntimeException(
          "SchemaParse ex Error compiling a file in " + schemaFileTree.asPath + " to " + outputDirectory,
          ex,
      )
    }
  }
}
