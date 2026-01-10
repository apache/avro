package eu.eventloopsoftware.avro.gradle.plugin.tasks

import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaParseException
import org.apache.avro.SchemaParser
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility
import org.apache.avro.generic.GenericData
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.TaskAction
import java.io.File
import java.io.IOException
import java.net.URL
import java.net.URLClassLoader

abstract class CompileAvroSchemaTask : AbstractCompileTask() {

    @get:InputFiles
    @get:SkipWhenEmpty
    abstract val schemaFiles: ConfigurableFileCollection

    @get:InputFiles
    @get:Classpath
    abstract val classpath: ConfigurableFileCollection

    @TaskAction
    fun compileSchema() {
        logger.info("Generating Java files from ${schemaFiles.files.size} Avro schemas...")

        compileSchemas(schemaFiles, outputDirectory.get().asFile)

        logger.info("Done generating Java files from Avro schemas...")
    }

    private fun compileSchemas(schemaFileTree: ConfigurableFileCollection, outputDirectory: File) {
        val sourceFileForModificationDetection: File? =
            schemaFileTree.asFileTree
                .files
                .filter { file: File -> file.lastModified() > 0 }
                .maxBy { it.lastModified() }


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

    private fun doCompile(
        sourceFileForModificationDetection: File?,
        compiler: SpecificCompiler,
        outputDirectory: File
    ) {
        setCompilerProperties(compiler)
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
        compiler.setTemplateDir(templateDirectory.get().asFile.absolutePath + "/")
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
            logger.warn("Could not parse field visibility, using PRIVATE")
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

    private fun loadLogicalTypesFactories() =
        createClassLoader().use { classLoader ->
            customLogicalTypeFactories.get().forEach { factory ->
                try {
                    @Suppress("UNCHECKED_CAST")
                    val logicalTypeFactoryClass =
                        classLoader.loadClass(factory) as Class<LogicalTypes.LogicalTypeFactory>
                    val factoryInstance = logicalTypeFactoryClass.getDeclaredConstructor().newInstance()
                    LogicalTypes.register(factoryInstance)
                } catch (e: ClassNotFoundException) {
                    throw IOException(e)
                } catch (e: ReflectiveOperationException) {
                    throw GradleException("Failed to instantiate logical type factory class: $factory", e)
                }
            }
        }

    private fun createClassLoader(): URLClassLoader {
        val urls = getClasspath()
        return URLClassLoader(urls.toTypedArray(), Thread.currentThread().contextClassLoader)
    }

    private fun getClasspath(): List<URL> = classpath.files.map { it.toURI().toURL() }


}
