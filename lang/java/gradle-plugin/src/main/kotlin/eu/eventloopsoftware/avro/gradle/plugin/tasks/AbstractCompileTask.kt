package eu.eventloopsoftware.avro.gradle.plugin.tasks

import org.apache.avro.LogicalTypes
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility
import org.apache.avro.generic.GenericData
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Classpath
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import java.io.File
import java.io.IOException
import java.net.URL
import java.net.URLClassLoader

abstract class AbstractCompileTask : DefaultTask() {

    @get:OutputDirectory
    abstract val outputDirectory: DirectoryProperty

    @get:Input
    abstract val fieldVisibility: Property<String>

    @get:Input
    abstract val testExcludes: ListProperty<String>

    @get:Input
    abstract val stringType: Property<String>

    @get:Input
    abstract val velocityToolsClassesNames: ListProperty<String>

    @get:Input
    abstract val templateDirectory: Property<String>

    @get:Input
    abstract val recordSpecificClass: Property<String>

    @get:Input
    abstract val errorSpecificClass: Property<String>

    @get:Input
    abstract val createOptionalGetters: Property<Boolean>

    @get:Input
    abstract val gettersReturnOptional: Property<Boolean>

    @get:Input
    abstract val optionalGettersForNullableFieldsOnly: Property<Boolean>

    @get:Input
    abstract val createSetters: Property<Boolean>

    @get:Input
    abstract val createNullSafeAnnotations: Property<Boolean>

    @get:Input
    abstract val nullSafeAnnotationNullable: Property<String>

    @get:Input
    abstract val nullSafeAnnotationNotNull: Property<String>

    @get:Input
    abstract val customConversions: ListProperty<String>

    @get:Input
    abstract val customLogicalTypeFactories: ListProperty<String>

    @get:Input
    abstract val enableDecimalLogicalType: Property<Boolean>

    @get:InputFiles
    @get:Classpath
    abstract val runtimeClassPathFileCollection: ConfigurableFileCollection

    protected fun doCompile(
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
        compiler.setTemplateDir(templateDirectory.get())
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
            logger.warn("Could not parse field visibility: ${fieldVisibility.get()}, using PRIVATE")
            return FieldVisibility.PRIVATE
        }
    }

    private fun instantiateAdditionalVelocityTools(velocityToolsClassesNames: List<String>): List<Any> {
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

    protected fun loadLogicalTypesFactories() =
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
        val urls = classPathFileCollection()
        return URLClassLoader(urls.toTypedArray(), Thread.currentThread().contextClassLoader)
    }

    private fun classPathFileCollection(): List<URL> =
        runtimeClassPathFileCollection.files.map { it.toURI().toURL() }


}
