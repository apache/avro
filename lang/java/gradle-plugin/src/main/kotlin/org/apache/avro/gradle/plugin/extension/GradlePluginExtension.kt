package org.apache.avro.gradle.plugin.extension

import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import javax.inject.Inject

abstract class GradlePluginExtension @Inject constructor(objects: ObjectFactory) {

    val schemaType = objects.property(String::class.java)

    val srcDirectory: Property<String> = objects.property(String::class.java)

    val outputDirectory: Property<String> = objects.property(String::class.java)

    val includes: ListProperty<String> = objects.listProperty(String::class.java)

    //// Input source directory
    //val sourceDir: DirectoryProperty = objects.directoryProperty()

}
