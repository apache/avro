package eu.eventloopsoftware.avro.gradle.plugin

import eu.eventloopsoftware.avro.gradle.plugin.extension.AvroGradlePluginExtension
import eu.eventloopsoftware.avro.gradle.plugin.tasks.AbstractCompileTask
import eu.eventloopsoftware.avro.gradle.plugin.tasks.CompileAvroSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.provider.Property
import org.gradle.api.tasks.TaskProvider
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmExtension

abstract class AvroGradlePlugin : Plugin<Project> {

    override fun apply(project: Project) {
        project.logger.info("Running Avro Gradle plugin for project: ${project.name}")

        val extension = project.extensions.create("avro", AvroGradlePluginExtension::class.java)

        // Required so that we can get the sourceSets from the java extension below.
        project.pluginManager.apply("java")

        val compileAvroSchemaTask = registerSchemaTask(extension, project)
        val compileTestAvroSchemaTask = registerSchemaTestTask(extension, project)
        addGeneratedSourcesHook(project, compileAvroSchemaTask, compileTestAvroSchemaTask)

    }

    private fun addGeneratedSourcesHook(
        project: Project,
        compileAvroSchemaTask: TaskProvider<CompileAvroSchemaTask>,
        compileTestAvroSchemaTask: TaskProvider<CompileAvroSchemaTask>
    ) {
        project.pluginManager.withPlugin("org.jetbrains.kotlin.jvm") {
            addGeneratedSourcesToKotlinProject(project, compileAvroSchemaTask, compileTestAvroSchemaTask)
        }

        project.plugins.withType(JavaPlugin::class.java) {
            addGeneratedSourcesToJavaProject(project, compileAvroSchemaTask, compileTestAvroSchemaTask)
        }
    }

    private fun registerSchemaTask(extension: AvroGradlePluginExtension, project: Project) =
        project.tasks.register("avroGenerateJavaClasses", CompileAvroSchemaTask::class.java) { compileSchemaTask ->

            val includesAvsc: Set<String> = extension.includedSchemaFiles.get()
            val includesProtocol: Set<String> = extension.includedProtocolFiles.get()

            addProperties(
                compileSchemaTask,
                extension,
                project,
                extension.outputDirectory
            )

            addSchemaFiles(compileSchemaTask, project, extension, includesAvsc, extension.sourceDirectory)
            addProtocolFiles(compileSchemaTask, project, extension, includesProtocol, extension.testSourceDirectory)

            compileSchemaTask.runtimeClassPathFileCollection.from(project.configurations.getByName("runtimeClasspath").files)
        }

    private fun registerSchemaTestTask(extension: AvroGradlePluginExtension, project: Project) =
        project.tasks.register(
            "avroGenerateTestJavaClasses",
            CompileAvroSchemaTask::class.java,
        ) { compileSchemaTask ->

            val includesAvsc: Set<String> = extension.includedSchemaFiles.get()
            val includesProtocol: Set<String> = extension.includedProtocolFiles.get()

            addProperties(
                compileSchemaTask,
                extension,
                project,
                extension.testOutputDirectory
            )

            addSchemaFiles(compileSchemaTask, project, extension, includesAvsc, extension.testSourceDirectory)
            addProtocolFiles(compileSchemaTask, project, extension, includesProtocol, extension.testSourceDirectory)

            compileSchemaTask.runtimeClassPathFileCollection.from(project.configurations.getByName("testRuntimeClasspath").files)
        }


    private fun addProperties(
        compileTask: AbstractCompileTask,
        extension: AvroGradlePluginExtension,
        project: Project,
        outputDirectory: Property<String>
    ) {
        compileTask.outputDirectory.set(project.layout.buildDirectory.dir(outputDirectory))
        compileTask.fieldVisibility.set(extension.fieldVisibility)
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

    private fun addSchemaFiles(
        compileSchemaTask: CompileAvroSchemaTask,
        project: Project,
        extension: AvroGradlePluginExtension,
        includes: Set<String>,
        sourceDirectory: Property<String>
    ) {
        compileSchemaTask.schemaFiles.from(project.fileTree(sourceDirectory).apply {
            setIncludes(includes)
            setExcludes(extension.excludes.get())
        })
        extension.sourceZipFiles.get().forEach { zipPath ->
            compileSchemaTask.schemaFiles.from(
                project.zipTree(zipPath).matching { it.include(includes) }
            )
        }
    }

    private fun addProtocolFiles(
        compileSchemaTask: CompileAvroSchemaTask,
        project: Project,
        extension: AvroGradlePluginExtension,
        includesProtocol: Set<String>,
        sourceDirectory: Property<String>
    ) {
        compileSchemaTask.protocolFiles.from(project.fileTree(sourceDirectory).apply {
            setIncludes(includesProtocol)
            setExcludes(extension.excludes.get())
        })
        extension.sourceZipFiles.get().forEach { zipPath ->
            compileSchemaTask.protocolFiles.from(
                project.zipTree(zipPath).matching { it.include(includesProtocol) }
            )
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



