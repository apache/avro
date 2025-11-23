package org.apache.avro.gradle.plugin.tasks

import org.apache.avro.Schema
import org.apache.avro.SchemaParseException
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.gradle.api.Project
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import java.io.File
import java.io.IOException
import java.util.Arrays
import java.util.Comparator
import java.util.function.Function
import java.util.stream.Collectors

abstract class CompileSchemaTask : AbstractCompileTask() {

    @TaskAction
    fun compileSchema() {
        println("Compiling schemas...")

        val sourceDirectoryFullPath =
            project.layout.projectDirectory.dir(srcDirectory.get()).asFile

        val outputDirectoryFullPath = project.layout.buildDirectory
            .dir(outputDirectory).get().asFile

        val excludes = emptyArray<String>()

        val res = project.getIncludedFiles2(
            sourcePath = srcDirectory.get(),
            excludes = excludes,
            includes = includes.get().toTypedArray()
        )

        //println("Included files: ${res.joinToString(",") }}")
        //println("sourceDir: ${sourceDirectoryFullPath.path}")
        //println("outputDir: ${outputDirectoryFullPath.path}")

        doCompile(res, sourceDirectoryFullPath, outputDirectoryFullPath)

        val files = File(outputDirectoryFullPath, "test").list().joinToString(",")

        println("here are all files: ${files}")

    }

    fun Project.getIncludedFiles2(
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
            throw RuntimeException("SchemaParse ex Error compiling a file in " + sourceDirectory + " to " + outputDirectory, ex)
        }
    }

    //    @Throws(IOException::class)
    private fun doCompile(
        sourceFileForModificationDetection: File,
        compiler: SpecificCompiler,
        outputDirectory: File
    ) {
        setCompilerProperties(compiler)
        // TODO: don't forget to re-add
//        try {
//            for (customConversion in customConversions) {
//                compiler.addCustomConversion(Thread.currentThread().getContextClassLoader().loadClass(customConversion))
//            }
//        } catch (e: ClassNotFoundException) {
//            throw IOException(e)
//        }
        compiler.compileToDestination(sourceFileForModificationDetection, outputDirectory)
    }

    protected fun setCompilerProperties(compiler: SpecificCompiler) {
//        compiler.setTemplateDir(templateDirectory)
//        compiler.setStringType(GenericData.StringType.valueOf(stringType))
//        compiler.setFieldVisibility(getFieldVisibility())
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
}
