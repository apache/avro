package org.apache.avro.gradle.plugin

import org.apache.avro.gradle.plugin.tasks.CompileSchemaTask
import org.gradle.api.Plugin
import org.gradle.api.Project

abstract class GradlePlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.tasks.register("compile", CompileSchemaTask::class.java)
    }
}
