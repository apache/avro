package org.apache.avro.gradle.plugin

import org.apache.avro.gradle.plugin.tasks.CompileSchemaTask
import org.gradle.testfixtures.ProjectBuilder
import kotlin.test.Test


class SamplePluginTest {
    @Test
    fun `plugin is applied correctly to the project`() {
        val project = ProjectBuilder.builder().build()
        project.pluginManager.apply("org.apache.avro.gradle.plugin")
        assert(project.tasks.getByName("compile") is CompileSchemaTask)
    }

}
