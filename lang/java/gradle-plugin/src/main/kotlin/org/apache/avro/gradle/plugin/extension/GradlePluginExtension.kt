package org.apache.avro.gradle.plugin.extension

import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import javax.inject.Inject

abstract class GradlePluginExtension @Inject constructor(objects: ObjectFactory) {

    /**
     * Schema type: "schema", "idl", "protocol" are valid. Default is "schema"
     */
    val schemaType = objects.property(String::class.java).convention("schema")

    /**
     * The source directory of avro files. This directory is added to the classpath
     * at schema compiling time. All files can therefore be referenced as classpath
     * resources following the directory structure under the source directory.
     *
     * @parameter property="sourceDirectory"
     *            default-value="${buildDirectory}/src/main/avro"
     */
    val sourceDirectory: Property<String> = objects.property(String::class.java).convention("src/main/avro")

    /**
     * @parameter property="outputDirectory"
     *            default-value="${buildDirectory}/generated-sources/avro"
     */
    val outputDirectory: Property<String> = objects.property(String::class.java).convention("generated-sources/avro")


    /**
     * @parameter property="sourceDirectory"
     * default-value="${project.layout.buildDirectory}/src/test/avro"
     */
    val testSourceDirectory: Property<String> = objects.property(String::class.java).convention("src/test/avro")

    /**
     * @parameter property="outputDirectory"
     * default-value="${project.layout.buildDirectory}/generated-test-sources/avro"
     */
    val testOutputDirectory: Property<String> =
        objects.property(String::class.java).convention("generated-test-sources/avro")


    /**
     * The field visibility indicator for the fields of the generated class, as
     * string values of SpecificCompiler.FieldVisibility. The text is case
     * insensitive.
     *
     * @parameter default-value="PRIVATE"
     */
    val fieldVisibility: Property<String> = objects.property(String::class.java).convention("PRIVATE")


    /**
     * A set of Ant-like exclusion patterns used to prevent certain files from being
     * processed. By default, this set is empty such that no files are excluded.
     *
     * @parameter
     */
    val excludes: ListProperty<String> = objects.listProperty(String::class.java).convention(emptyList())

    /**
     * A set of Ant-like exclusion patterns used to prevent certain files from being
     * processed. By default, this set is empty such that no files are excluded.
     *
     * @parameter
     */
    val testExcludes: ListProperty<String> = objects.listProperty(String::class.java).convention(emptyList())

    /**
     * The Java type to use for Avro strings. May be one of CharSequence, String or
     * Utf8. String by default.
     *
     * @parameter property="stringType"
     */
    val stringType: Property<String> = objects.property(String::class.java).convention("String")


    /**
     * A set of fully qualified class names of custom
     * {@link org.apache.avro.Conversion} implementations to add to the compiler.
     * The classes must be on the classpath at compile time and whenever the Java
     * objects are serialized.
     *
     * @parameter property="customConversions"
     */
    val customConversions: ListProperty<String> = objects.listProperty(String::class.java).convention(emptyList())


    /**
     * The directory (within the java classpath) that contains the velocity
     * templates to use for code generation. The default value points to the
     * templates included with the avro-maven-plugin.
     *
     * @parameter property="templateDirectory"
     */
    val templateDirectory: Property<String> =
        objects.property(String::class.java).convention("/org/apache/avro/compiler/specific/templates/java/classic/")


    /**
     * A set of fully qualified class names of custom
     * [org.apache.avro.LogicalTypes.LogicalTypeFactory] implementations to
     * add to the compiler. The classes must be on the classpath at compile time and
     * whenever the Java objects are serialized.
     *
     * @parameter property="customLogicalTypeFactories"
     */

    val customLogicalTypeFactories: ListProperty<String> =
        objects.listProperty(String::class.java).convention(emptyList())

}
