package org.apache.avro.gradle.plugin

import org.apache.avro.gradle.plugin.extension.GradlePluginExtension
import org.apache.avro.gradle.plugin.tasks.CompileSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.internal.cc.base.logger

abstract class GradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        logger.info("Running Avro Gradle plugin for project: ${project.name}")

        val extension: GradlePluginExtension = project.extensions.create("avro", GradlePluginExtension::class.java)

        project.pluginManager.apply("java")

        project.tasks.register("avroGenerateJavaClasses", CompileSchemaTask::class.java) {
            val schemaType: SchemaType = SchemaType.valueOf(extension.schemaType.get())

            when (schemaType) {
                SchemaType.schema -> {
                    it.source(extension.sourceDirectory)
                    it.sourceDirectory.set(extension.sourceDirectory)
                    it.outputDirectory.set(extension.outputDirectory)
                    it.testSourceDirectory.set(extension.testSourceDirectory)
                    it.testOutputDirectory.set(extension.testOutputDirectory)
                    it.fieldVisibility.set(extension.fieldVisibility)
                    it.setExcludes(extension.excludes.get().toSet())
                    it.setIncludes(setOf("**/*.avsc"))
                    it.testExcludes.set(extension.testExcludes)
                    it.stringType.set(extension.stringType)
                    it.templateDirectory.set(extension.templateDirectory)
                    it.recordSpecificClass.set(extension.recordSpecificClass)
                    it.errorSpecificClass.set(extension.errorSpecificClass)
                    it.createOptionalGetters.set(extension.createOptionalGetters)
                    it.gettersReturnOptional.set(extension.gettersReturnOptional)
                    it.createSetters.set(extension.createSetters)
                    it.createNullSafeAnnotations.set(extension.createNullSafeAnnotations)
                    it.optionalGettersForNullableFieldsOnly.set(extension.optionalGettersForNullableFieldsOnly)
                    it.customConversions.set(extension.customConversions)
                    it.customLogicalTypeFactories.set(extension.customLogicalTypeFactories)
                    it.enableDecimalLogicalType.set(extension.enableDecimalLogicalType)

                    addGeneratedSourcesToProject(project, it.outputDirectory.get())
                }

                SchemaType.idl -> TODO()
            }

        }
    }

    private fun addGeneratedSourcesToProject(project: Project, outputDirectory: String) {
        val sourceSets = project.extensions.getByType(JavaPluginExtension::class.java).sourceSets
        val generatedSourcesDir = project.layout.buildDirectory.dir(outputDirectory)
        project.logger.debug("Generated sources directory: ${generatedSourcesDir.get()}")

        // Add directory that contains the generated Java files to source set
        sourceSets.getByName("main").java.srcDir(generatedSourcesDir)
        project.dependencies.add("implementation", "org.apache.avro:avro:1.13.0-SNAPSHOT")
    }
}

enum class SchemaType {
    schema,
    idl
}
