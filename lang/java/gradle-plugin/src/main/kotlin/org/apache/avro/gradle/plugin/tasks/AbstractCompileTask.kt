package org.apache.avro.gradle.plugin.tasks

import org.gradle.api.DefaultTask
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input

abstract class AbstractCompileTask : DefaultTask() {


    @get:Input
    abstract val sourceDirectory: Property<String>

    @get:Input
    abstract val outputDirectory: Property<String>

    @get:Input
    abstract val testSourceDirectory: Property<String>

    @get:Input
    abstract val testOutputDirectory: Property<String>

    @get:Input
    abstract val fieldVisibility: Property<String>

    @get:Input
    abstract val excludes: ListProperty<String>

    @get:Input
    abstract val testExcludes: ListProperty<String>

    @get:Input
    abstract val stringType: Property<String>

    @get:Input
    abstract val templateDirectory: Property<String>

    @get:Input
    abstract val recordSpecificClass: Property<String>

    @get:Input
    abstract val errorSpecificClass: Property<String>

    @get:Input
    abstract val createOptionalGetters: Property<Boolean>

    @get:Input
    abstract val gettersReturnOptional: Property<Boolean>

    @get:Input
    abstract val optionalGettersForNullableFieldsOnly: Property<Boolean>

    @get:Input
    abstract val customConversions: ListProperty<String>

    @get:Input
    abstract val customLogicalTypeFactories: ListProperty<String>

}
