package eu.eventloopsoftware.avro.gradle.plugin.tasks

import org.apache.avro.SchemaParseException
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility
import org.apache.avro.generic.GenericData
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.TaskAction
import java.io.File
import java.io.IOException

abstract class CompileAvroSchemaTask : AbstractCompileTask() {

    @TaskAction
    fun compileSchema() {
        project.logger.info("Generating Java files from Avro schemas...")

        if (!source.isEmpty) {
            val sourceDirectoryFullPath = sourceDirectory.get().asFile
            val outputDirectoryFullPath = outputDirectory.get().asFile
            compileSchemas(source, sourceDirectoryFullPath, outputDirectoryFullPath)
        } else {
            logger.warn("No Avro files found in $sourceDirectory. Nothing to compile")
        }

        project.logger.info("Done generating Java files from Avro schemas...")
    }

    private fun compileSchemas(fileTree: FileTree, sourceDirectory: File, outputDirectory: File) {
        val sourceFileForModificationDetection: File? =
            fileTree
                .files
                .filter { file: File -> file.lastModified() > 0 }
                .maxBy { it.lastModified() }

        try {
            val parser = SchemaParser()
            for (sourceFile in fileTree.files) {
                parser.parse(sourceFile)
            }
            val schemas = parser.parsedNamedSchemas

            doCompile(sourceFileForModificationDetection, SpecificCompiler(schemas), outputDirectory)
        } catch (ex: IOException) {
            // TODO: more concrete exceptions
            throw RuntimeException("IO ex: Error compiling a file in " + sourceDirectory + " to " + outputDirectory, ex)
        } catch (ex: SchemaParseException) {
            throw RuntimeException(
                "SchemaParse ex Error compiling a file in " + sourceDirectory + " to " + outputDirectory,
                ex
            )
        }
    }

    private fun doCompile(
        sourceFileForModificationDetection: File?,
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
        compiler.setTemplateDir(project.layout.projectDirectory.dir(templateDirectory.get()).asFile.absolutePath + "/")
        compiler.setStringType(GenericData.StringType.valueOf(stringType.get()))
        compiler.setFieldVisibility(getFieldV())
        compiler.setCreateOptionalGetters(createOptionalGetters.get())
        compiler.setGettersReturnOptional(gettersReturnOptional.get())
        compiler.setOptionalGettersForNullableFieldsOnly(optionalGettersForNullableFieldsOnly.get())
        compiler.setCreateSetters(createSetters.get())
        compiler.setCreateNullSafeAnnotations(createNullSafeAnnotations.get())
        compiler.setNullSafeAnnotationNullable(nullSafeAnnotationNullable.get())
        compiler.setNullSafeAnnotationNotNull(nullSafeAnnotationNotNull.get())
        compiler.setEnableDecimalLogicalType(enableDecimalLogicalType.get())
        // TODO: likely not needed
//        compiler.setOutputCharacterEncoding(project.getProperties().getProperty("project.build.sourceEncoding"))
        compiler.setAdditionalVelocityTools(instantiateAdditionalVelocityTools(velocityToolsClassesNames.get()))
        compiler.setRecordSpecificClass(recordSpecificClass.get())
        compiler.setErrorSpecificClass(errorSpecificClass.get())
    }

    private fun getFieldV(): FieldVisibility {
        try {
            val upperCaseFieldVisibility = fieldVisibility.get().trim().uppercase()
            return FieldVisibility.valueOf(upperCaseFieldVisibility)
        } catch (_: IllegalArgumentException) {
            project.logger.warn("Could not parse field visibility, using PRIVATE")
            return FieldVisibility.PRIVATE
        }
    }

    protected fun instantiateAdditionalVelocityTools(velocityToolsClassesNames: List<String>): List<Any> {
        return velocityToolsClassesNames.map { velocityToolClassName ->
            try {
                Class.forName(velocityToolClassName)
                    .getDeclaredConstructor()
                    .newInstance()
            } catch (e: Exception) {
                throw RuntimeException(e)
            }
        }
    }
}
