package eu.eventloopsoftware.avro.gradle.plugin

import eu.eventloopsoftware.avro.gradle.plugin.extension.AvroGradlePluginExtension
import eu.eventloopsoftware.avro.gradle.plugin.tasks.CompileAvroSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.TaskProvider
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmExtension


abstract class AvroGradlePlugin : Plugin<Project> {

    override fun apply(project: Project) {
        project.logger.info("Running Avro Gradle plugin for project: ${project.name}")

        val extension = project.extensions.create("avro", AvroGradlePluginExtension::class.java)

        // Required so that we can get the sourceSets from the java extension below.
        project.pluginManager.apply("java")

        val compileAvroSchemaTask =
            project.tasks.register("avroGenerateJavaClasses", CompileAvroSchemaTask::class.java) { compileSchemaTask ->
                val sourceDirectory = extension.sourceDirectory.get()
                val outputDirectory = extension.outputDirectory.get()
                configurePlugin(compileSchemaTask, extension, project, sourceDirectory, outputDirectory)
            }

        val compileTestAvroSchemaTask = project.tasks.register(
            "avroGenerateTestJavaClasses",
            CompileAvroSchemaTask::class.java
        ) { compileSchemaTask ->
            val sourceDirectory = extension.testSourceDirectory.get()
            val outputDirectory = extension.testOutputDirectory.get()
            configurePlugin(compileSchemaTask, extension, project, sourceDirectory, outputDirectory)
        }

        // Add generated code before compilation
        project.pluginManager.withPlugin("org.jetbrains.kotlin.jvm") {
            addGeneratedSourcesToKotlinProject(project, compileAvroSchemaTask, compileTestAvroSchemaTask)
        }

        project.plugins.withType(JavaPlugin::class.java) {
            addGeneratedSourcesToJavaProject(project, compileAvroSchemaTask, compileTestAvroSchemaTask)
        }
    }

    private fun configurePlugin(
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

            }

            SchemaType.protocol -> TODO()
        }
    }

    private fun addGeneratedSourcesToJavaProject(
        project: Project,
        compileTask: TaskProvider<CompileAvroSchemaTask>,
        compileTestTask: TaskProvider<CompileAvroSchemaTask>
    ) {
        val sourceSets = project.extensions.getByType(JavaPluginExtension::class.java).sourceSets
        sourceSets.getByName("main").java.srcDir(compileTask.flatMap { it.outputDirectory })
        sourceSets.getByName("test").java.srcDir(compileTestTask.flatMap { it.outputDirectory })
    }

    private fun addGeneratedSourcesToKotlinProject(
        project: Project,
        compileTask: TaskProvider<CompileAvroSchemaTask>,
        compileTestTask: TaskProvider<CompileAvroSchemaTask>,
    ) {
        val sourceSets = project.extensions.getByType(KotlinJvmExtension::class.java).sourceSets
        sourceSets.getByName("main").kotlin.srcDir(compileTask.flatMap { it.outputDirectory })
        sourceSets.getByName("test").kotlin.srcDir(compileTestTask.flatMap { it.outputDirectory })
    }
}

enum class SchemaType {
    schema,
    protocol
}
