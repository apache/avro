package org.apache.avro.gradle.plugin

import org.apache.avro.gradle.plugin.extension.GradlePluginExtension
import org.apache.avro.gradle.plugin.tasks.CompileSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.internal.cc.base.logger
import kotlin.collections.toSet

abstract class GradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        logger.info("Running Avro Gradle plugin for project: ${project.name}")

        val extension: GradlePluginExtension = project.extensions.create("avro", GradlePluginExtension::class.java)

        project.pluginManager.apply("java")

        project.tasks.register("avroGenerateJavaClasses", CompileSchemaTask::class.java) {
            val sourceDirectory = extension.sourceDirectory.get()
            val outputDirectory = extension.outputDirectory.get()
            runPlugin(it, extension, project, sourceDirectory, outputDirectory)

        }

        project.tasks.register("avroGenerateTestJavaClasses", CompileSchemaTask::class.java) {
            val sourceDirectory = extension.testSourceDirectory.get()
            val outputDirectory = extension.testOutputDirectory.get()
            runPlugin(it, extension, project, sourceDirectory, outputDirectory)
        }

    }

    private fun runPlugin(
        compileTask: CompileSchemaTask,
        extension: GradlePluginExtension,
        project: Project,
        sourceDirectory: String,
        outputDirectory: String
    ) {
        val schemaType: SchemaType = SchemaType.valueOf(extension.schemaType.get())

        when (schemaType) {
            SchemaType.schema -> {
                compileTask.source(project.fileTree(sourceDirectory))
                compileTask.sourceDirectory.set(sourceDirectory)
                compileTask.outputDirectory.set(outputDirectory)
                //compileTask.testSourceDirectory.set(extension.testSourceDirectory)
                //compileTask.testOutputDirectory.set(extension.testOutputDirectory)
                compileTask.fieldVisibility.set(extension.fieldVisibility)
                compileTask.setExcludes(extension.excludes.get().toSet())
                compileTask.setIncludes(setOf("**/*.avsc"))
                compileTask.testExcludes.set(extension.testExcludes)
                compileTask.stringType.set(extension.stringType)
                compileTask.templateDirectory.set(extension.templateDirectory)
                compileTask.recordSpecificClass.set(extension.recordSpecificClass)
                compileTask.errorSpecificClass.set(extension.errorSpecificClass)
                compileTask.createOptionalGetters.set(extension.createOptionalGetters)
                compileTask.gettersReturnOptional.set(extension.gettersReturnOptional)
                compileTask.createSetters.set(extension.createSetters)
                compileTask.createNullSafeAnnotations.set(extension.createNullSafeAnnotations)
                compileTask.optionalGettersForNullableFieldsOnly.set(extension.optionalGettersForNullableFieldsOnly)
                compileTask.customConversions.set(extension.customConversions)
                compileTask.customLogicalTypeFactories.set(extension.customLogicalTypeFactories)
                compileTask.enableDecimalLogicalType.set(extension.enableDecimalLogicalType)

                addGeneratedSourcesToProject(project, compileTask.outputDirectory.get())
            }

            SchemaType.idl -> TODO()
        }
    }

    private fun addGeneratedSourcesToProject(project: Project, outputDirectory: String) {
        val sourceSets = project.extensions.getByType(JavaPluginExtension::class.java).sourceSets
        val generatedSourcesDir = project.layout.buildDirectory.dir(outputDirectory)
        project.logger.debug("Generated sources directory: ${generatedSourcesDir.get()}")

        // Add directory that contains the generated Java files to source set
        sourceSets.getByName("main").java.srcDir(generatedSourcesDir)
    }
}

enum class SchemaType {
    schema,
    idl
}
