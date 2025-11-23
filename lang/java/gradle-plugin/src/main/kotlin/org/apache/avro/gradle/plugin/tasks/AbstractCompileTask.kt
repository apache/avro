package org.apache.avro.gradle.plugin.tasks

import org.gradle.api.DefaultTask
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input

abstract class AbstractCompileTask : DefaultTask() {

    /**
     * A set of Ant-like inclusion patterns used to select files from the source
     * directory for processing. By default, the pattern `**&#47;*.avdl`
     * is used to select IDL files.
     *
     * @parameter
     */
    @get:Input
    abstract val includes: ListProperty<String>

    @get:Input
    abstract val srcDirectory: Property<String>

    @get:Input
    abstract val outputDirectory: Property<String>

}
