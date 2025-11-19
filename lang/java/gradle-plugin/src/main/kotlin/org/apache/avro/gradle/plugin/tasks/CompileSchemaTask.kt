package org.apache.avro.gradle.plugin.tasks

import org.apache.avro.Schema
import org.apache.avro.SchemaParseException
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import java.io.File
import java.io.IOException
import java.util.*
import java.util.function.Function
import java.util.stream.Collectors

abstract class CompileSchemaTask : AbstractCompileTask() {
    /**
     * A parser used to parse all schema files. Using a common parser will
     * facilitate the import of external schemas.
     */
    private val schemaParser = SchemaParser()
    /**
     * A set of Ant-like inclusion patterns used to select files from the source
     * directory for processing. By default, the pattern `**&#47;*.avdl`
     * is used to select IDL files.
     *
     * @parameter
     */
    private val includes = arrayOf("**/*.avdl")

    /**
     * A set of Ant-like inclusion patterns used to select files from the source
     * directory for processing. By default, the pattern `**&#47;*.avdl`
     * is used to select IDL files.
     *
     * @parameter
     */
    private val testIncludes = arrayOf("**/*.avdl")

    @get:Input
    abstract val gradleExtensionProperty: Property<String>

    @TaskAction
    fun compileSchema() {
        println("Starting compilation for project name: '${project.name}'")

    }
}
