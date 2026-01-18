package eu.eventloopsoftware.avro.gradle.plugin.tasks

import org.apache.avro.Protocol
import org.apache.avro.SchemaParseException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.TaskAction
import java.io.File
import java.io.IOException

abstract class CompileAvroProtocolTask : AbstractCompileTask() {

    @get:InputFiles
    @get:SkipWhenEmpty
    abstract val schemaFiles: ConfigurableFileCollection

    @get:Input
    abstract val other: String

    @TaskAction
    fun compileProtocol() {
        logger.info("Generating Java files from ${schemaFiles.files.size} Avro Protocol files...")

        compileAvroFiles(schemaFiles, outputDirectory.get().asFile)

        logger.info("Done generating Java files from Avro Protocol files...")
    }

    private fun compileAvroFiles(schemaFileTree: ConfigurableFileCollection, outputDirectory: File) {
        // Need to register custom logical type factories before schema compilation.
        try {
            loadLogicalTypesFactories()
        } catch (e: IOException) {
            throw RuntimeException("Error while loading logical types factories ", e)
        }

        try {
            for (sourceFile in schemaFileTree.files) {
                val protocol = Protocol.parse(sourceFile)
                doCompile(sourceFile, protocol, outputDirectory)
            }
        } catch (ex: IOException) {
            // TODO: more concrete exceptions
            throw RuntimeException(
                "IO ex: Error compiling a file in " + schemaFileTree.asPath + " to " + outputDirectory,
                ex
            )
        } catch (ex: SchemaParseException) {
            throw RuntimeException(
                "SchemaParse ex Error compiling a file in " + schemaFileTree.asPath + " to " + outputDirectory,
                ex
            )
        }
    }

}
