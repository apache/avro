package org.apache.avro.gradle.plugin

import org.apache.avro.gradle.plugin.extension.GradlePluginExtension
import org.apache.avro.gradle.plugin.tasks.CompileSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project

abstract class GradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        println("Applying avro Gradle plugin")

        val extension = project.extensions.create("avro", GradlePluginExtension::class.java)

        project.tasks.register("avroGenerateJavaClasses", CompileSchemaTask::class.java) {

            val schemaType: SchemaType = SchemaType.valueOf(extension.schemaType.get())

            when (schemaType) {
                SchemaType.schema -> {
                    it.sourceDirectory.set(extension.sourceDirectory)
                    it.outputDirectory.set(extension.outputDirectory)
                    it.testSourceDirectory.set(extension.testSourceDirectory)
                    it.testOutputDirectory.set(extension.testOutputDirectory)
                    it.fieldVisibility.set(extension.fieldVisibility)
                    it.excludes.set(extension.excludes)
                    it.testExcludes.set(extension.testExcludes)
                    it.stringType.set(extension.stringType)
                    it.templateDirectory.set(extension.templateDirectory)
                    it.customConversions.set(extension.customConversions)
                    it.customLogicalTypeFactories.set(extension.customLogicalTypeFactories)

                    //it.includes.set(extension.includes)
                    it.doFirst { println("Starting compilation for project name: '${project.name}'") }
                    it.doLast { println("Finished compilation for project name: '${project.name}'") }
                }

                SchemaType.idl -> TODO()
            }

        }
    }
}

enum class SchemaType {
    schema,
    idl
}
