package eu.eventloopsoftware.avro.gradle.plugin

import eu.eventloopsoftware.avro.gradle.plugin.extension.AvroGradlePluginExtension
import eu.eventloopsoftware.avro.gradle.plugin.tasks.CompileAvroSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension

abstract class GradlePlugin : Plugin<Project> {

    override fun apply(project: Project) {
        project.logger.info("Running Avro Gradle plugin for project: ${project.name}")

        val extension: AvroGradlePluginExtension =
            project.extensions.create("avro", AvroGradlePluginExtension::class.java)

        // Required so that we can get the sourceSets from the java extension below.
        project.pluginManager.apply("java")

        project.tasks.register("avroGenerateJavaClasses", CompileAvroSchemaTask::class.java) { compileSchemaTask ->
            val sourceDirectory = extension.sourceDirectory.get()
            val outputDirectory = extension.outputDirectory.get()
            runPlugin(compileSchemaTask, extension, project, sourceDirectory, outputDirectory)
        }

        project.tasks.register("avroGenerateTestJavaClasses", CompileAvroSchemaTask::class.java) { compileSchemaTask ->
            val sourceDirectory = extension.testSourceDirectory.get()
            val outputDirectory = extension.testOutputDirectory.get()
            runPlugin(compileSchemaTask, extension, project, sourceDirectory, outputDirectory)
        }
    }

    private fun runPlugin(
        compileTask: CompileAvroSchemaTask,
        extension: AvroGradlePluginExtension,
        project: Project,
        sourceDirectory: String,
        outputDirectory: String
    ) {
        val schemaType: SchemaType = SchemaType.valueOf(extension.schemaType.get())

        when (schemaType) {
            SchemaType.schema -> {
                compileTask.source(sourceDirectory)
                compileTask.sourceDirectory.set(project.layout.projectDirectory.dir(sourceDirectory))
                compileTask.outputDirectory.set(project.layout.buildDirectory.dir(outputDirectory))
                compileTask.fieldVisibility.set(extension.fieldVisibility)
                compileTask.setExcludes(extension.excludes.get().toSet())
                compileTask.setIncludes(setOf("**/*.avsc"))
                compileTask.testExcludes.set(extension.testExcludes)
                compileTask.stringType.set(extension.stringType)
                compileTask.velocityToolsClassesNames.set(extension.velocityToolsClassesNames.get())
                compileTask.templateDirectory.set(extension.templateDirectory)
                compileTask.recordSpecificClass.set(extension.recordSpecificClass)
                compileTask.errorSpecificClass.set(extension.errorSpecificClass)
                compileTask.createOptionalGetters.set(extension.createOptionalGetters)
                compileTask.gettersReturnOptional.set(extension.gettersReturnOptional)
                compileTask.createSetters.set(extension.createSetters)
                compileTask.createNullSafeAnnotations.set(extension.createNullSafeAnnotations)
                compileTask.nullSafeAnnotationNullable.set(extension.nullSafeAnnotationNullable)
                compileTask.nullSafeAnnotationNotNull.set(extension.nullSafeAnnotationNotNull)
                compileTask.optionalGettersForNullableFieldsOnly.set(extension.optionalGettersForNullableFieldsOnly)
                compileTask.customConversions.set(extension.customConversions)
                compileTask.customLogicalTypeFactories.set(extension.customLogicalTypeFactories)
                compileTask.enableDecimalLogicalType.set(extension.enableDecimalLogicalType)

                addGeneratedSourcesToProject(project, compileTask)
            }

            SchemaType.protocol -> TODO()
        }
    }

    private fun addGeneratedSourcesToProject(project: Project, compileTask: CompileAvroSchemaTask) {
        val sourceSets = project.extensions.getByType(JavaPluginExtension::class.java).sourceSets
        sourceSets.getByName("main").java.srcDir(compileTask.outputDirectory)
    }
}

enum class SchemaType {
    schema,
    protocol
}
