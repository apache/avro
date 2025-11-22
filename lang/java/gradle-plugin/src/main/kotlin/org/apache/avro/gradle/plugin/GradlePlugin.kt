package org.apache.avro.gradle.plugin

import org.apache.avro.gradle.plugin.extension.GradlePluginExtension
import org.apache.avro.gradle.plugin.tasks.CompileSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project

abstract class GradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        val extension = project.extensions.create("avro", GradlePluginExtension::class.java)

        project.tasks.register("compile", CompileSchemaTask::class.java) {
            it.srcDirectory.set(extension.srcDirectory)
            it.outputDirectory.set(extension.outputDirectory)
            it.includes.set(extension.includes)
            it.doFirst { println("Starting compilation for project name: '${project.name}'") }
            it.doLast { println("Finished compilation for project name: '${project.name}'") }
        }
    }
}
