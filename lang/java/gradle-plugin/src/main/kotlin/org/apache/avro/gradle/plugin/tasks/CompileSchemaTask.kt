package org.apache.avro.gradle.plugin.tasks

import org.apache.avro.Schema
import org.apache.avro.SchemaParseException
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility
import org.gradle.api.Project
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
     * A set of Ant-like inclusion patterns used to select files from the source
     * directory for processing. By default, the pattern `**&#47;*.avdl`
     * is used to select IDL files.
     *
     * @parameter
     */
    @get:Input
    val includes: List<String> = listOf("**/*.avsc")

    @TaskAction
    fun compileSchema() {
        project.logger.info("Compile schema")
        println("Compiling schemas...")

        val sourceDirectoryFullPath = getSourceDirectoryFullPath(sourceDirectory)
        val outputDirectoryFullPath = getBuildDirectoryFullPath(outputDirectory)

        val testSourceDirectoryFullPath = getSourceDirectoryFullPath(testSourceDirectory)
        val testOutputDirectoryFullPath = getBuildDirectoryFullPath(testOutputDirectory)

        val avroFiles = project.getIncludedFiles(
            sourcePath = sourceDirectory.get(),
            excludes = excludes.get().toTypedArray(),
            includes = includes.toTypedArray()
        )

        val avroTestFiles = project.getIncludedFiles(
            sourcePath = testSourceDirectory.get(),
            excludes = testExcludes.get().toTypedArray(),
            includes = includes.toTypedArray()
        )

        //println("Included files: ${avroFiles.joinToString(",")}}")
        //println("sourceDir: ${sourceDirectoryFullPath.path}")
        //println("outputDir: ${outputDirectoryFullPath.path}")

        doCompile(avroFiles, sourceDirectoryFullPath, outputDirectoryFullPath)

        doCompile(avroTestFiles, testSourceDirectoryFullPath, testOutputDirectoryFullPath)

        //val files = File(outputDirectoryFullPath, "test").list().joinToString(",")

        //println("here are all files: ${files}")

    }

    private fun getSourceDirectoryFullPath(directoryProperty: Property<String>): File =
        project.layout.projectDirectory.dir(directoryProperty.get()).asFile

    private fun getBuildDirectoryFullPath(directoryProperty: Property<String>): File =
        project.layout.buildDirectory.dir(directoryProperty).get().asFile

    fun Project.getIncludedFiles(
        sourcePath: String,
        excludes: Array<String>,
        includes: Array<String>
    ): Array<String> {
        println("Including files from path: $sourcePath")

        val fullPath = project.layout.projectDirectory.dir(sourcePath)

        val files = fileTree(fullPath) {
            it.include(*includes)
            it.exclude(*excludes)
        }

        return files.files
            .map { it.relativeTo(fullPath.asFile).path }
            .toTypedArray()
    }

    protected fun doCompile(fileNames: Array<String>, sourceDirectory: File, outputDirectory: File) {
        val sourceFiles: List<File> = Arrays.stream(fileNames)
            .map { filename: String -> File(sourceDirectory, filename) }.collect(Collectors.toList())
        val sourceFileForModificationDetection =
            sourceFiles.stream().filter { file: File? -> file!!.lastModified() > 0 }
                .max(Comparator.comparing(Function { obj: File? -> obj!!.lastModified() })).orElse(null)
        val schemas: MutableList<Schema?>?

        try {
            val parser = SchemaParser()
            for (sourceFile in sourceFiles) {
                parser.parse(sourceFile)
            }
            schemas = parser.parsedNamedSchemas

            doCompile(sourceFileForModificationDetection, SpecificCompiler(schemas), outputDirectory)
        } catch (ex: IOException) {
            throw RuntimeException("IO ex: Error compiling a file in " + sourceDirectory + " to " + outputDirectory, ex)
        } catch (ex: SchemaParseException) {
            throw RuntimeException(
                "SchemaParse ex Error compiling a file in " + sourceDirectory + " to " + outputDirectory,
                ex
            )
        }
    }


    private fun doCompile(
        sourceFileForModificationDetection: File,
        compiler: SpecificCompiler,
        outputDirectory: File
    ) {
        setCompilerProperties(compiler)
        // TODO:
        //  * customLogicalTypeFactories

        try {
            for (customConversion in customConversions.get()) {
                compiler.addCustomConversion(Thread.currentThread().getContextClassLoader().loadClass(customConversion))
            }
        } catch (e: ClassNotFoundException) {
            throw IOException(e)
        }
        compiler.compileToDestination(sourceFileForModificationDetection, outputDirectory)
    }


    private fun setCompilerProperties(compiler: SpecificCompiler) {
//        compiler.setTemplateDir(templateDirectory)
//        compiler.setStringType(GenericData.StringType.valueOf(stringType))
        compiler.setFieldVisibility(getFv())
//        compiler.setCreateOptionalGetters(createOptionalGetters)
//        compiler.setGettersReturnOptional(gettersReturnOptional)
//        compiler.setOptionalGettersForNullableFieldsOnly(optionalGettersForNullableFieldsOnly)
//        compiler.setCreateSetters(createSetters)
//        compiler.setCreateNullSafeAnnotations(createNullSafeAnnotations)
//        compiler.setNullSafeAnnotationNullable(nullSafeAnnotationNullable)
//        compiler.setNullSafeAnnotationNotNull(nullSafeAnnotationNotNull)
//        compiler.setEnableDecimalLogicalType(enableDecimalLogicalType)
//        compiler.setOutputCharacterEncoding(project.getProperties().getProperty("project.build.sourceEncoding"))
//        compiler.setAdditionalVelocityTools(instantiateAdditionalVelocityTools())
//        compiler.setRecordSpecificClass(this.recordSpecificClass)
//        compiler.setErrorSpecificClass(this.errorSpecificClass)
    }

    private fun getFv(): FieldVisibility {
        try {
            val upperCaseFieldVisibility = fieldVisibility.get().trim().uppercase(Locale.getDefault())
            return FieldVisibility.valueOf(upperCaseFieldVisibility)
        } catch (e: IllegalArgumentException) {
            logger.warn("Could not parse field visibility, using PRIVATE")
            return FieldVisibility.PRIVATE
        }
    }
}
