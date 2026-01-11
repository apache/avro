package eu.eventloopsoftware.avro.gradle.plugin.extension

import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import javax.inject.Inject

abstract class AvroGradlePluginExtension @Inject constructor(objects: ObjectFactory) {

    /**
     * Schema type: "schema", "idl", "protocol" are valid. Default is "schema"
     */
    val schemaType: Property<String> = objects.property(String::class.java).convention("schema")

    /**
     * The source directory containing Avro schema files.
     * <p>
     * Defaults to {@code src/main/avro}.
     */
    val sourceDirectory: Property<String> = objects.property(String::class.java).convention("src/main/avro")

    /**
     * A list of zip files that contain Avro schema files. All generated
     *  Java classes are added to the classpath.
     * <p>
     * Defaults to {@code emptyList()}.
     */
    val sourceZipFiles: ListProperty<String> = objects.listProperty(String::class.java).convention(emptyList())

    /**
     * The output directory for the generated Java code.
     */
    val outputDirectory: Property<String> = objects.property(String::class.java).convention("generated-sources-avro")


    /**
     * The output directory for the generated test Java code.
     */
    val testSourceDirectory: Property<String> = objects.property(String::class.java).convention("src/test/avro")

    /**
     * @parameter property="outputDirectory"
     * default-value="${project.layout.buildDirectory}/generated-test-sources/avro"
     */
    val testOutputDirectory: Property<String> =
        objects.property(String::class.java).convention("generated-test-sources-avro")


    /**
     * The field visibility indicator for the fields of the generated class, as
     * string values of SpecificCompiler.FieldVisibility. The text is case
     * insensitive.
     *
     * @parameter default-value="PRIVATE"
     */
    val fieldVisibility: Property<String> = objects.property(String::class.java).convention("PRIVATE")


    /**
     * A set of Ant-like inclusion patterns used to select files from the source
     * directory for processing. The default pattern is different for Schema,
     * Protocol and IDL files.
     *
     * @parameter
     */
    val includes: ListProperty<String> = objects.listProperty(String::class.java).convention(emptyList())

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
     * The qualified names of classes which the plugin will look up, instantiate
     * (through an empty constructor that must exist) and set up to be injected into
     * Velocity templates by Avro compiler.
     *
     * @parameter property="velocityToolsClassesNames"
     */
    val velocityToolsClassesNames: ListProperty<String> =
        objects.listProperty(String::class.java).convention(emptyList())


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
     * Generated record schema classes will extend this class.
     *
     * @parameter property="recordSpecificClass"
     */
    val recordSpecificClass: Property<String> =
        objects.property(String::class.java).convention("org.apache.avro.specific.SpecificRecordBase")


    /**
     * Generated error schema classes will extend this class.
     *
     * @parameter property="errorSpecificClass"
     */
    val errorSpecificClass: Property<String> =
        objects.property(String::class.java).convention("org.apache.avro.specific.SpecificExceptionBase")


    /**
     * The createOptionalGetters parameter enables generating the getOptional...
     * methods that return an Optional of the requested type. This works ONLY on
     * Java 8+
     *
     * @parameter property="createOptionalGetters"
     */
    val createOptionalGetters: Property<Boolean> = objects.property(Boolean::class.java).convention(false)

    /**
     * The gettersReturnOptional parameter enables generating get... methods that
     * return an Optional of the requested type. This works ONLY on Java 8+
     *
     * @parameter property="gettersReturnOptional"
     */
    val gettersReturnOptional: Property<Boolean> = objects.property(Boolean::class.java).convention(false)

    /**
     * The optionalGettersForNullableFieldsOnly parameter works in conjunction with
     * gettersReturnOptional option. If it is set, Optional getters will be
     * generated only for fields that are nullable. If the field is mandatory,
     * regular getter will be generated. This works ONLY on Java 8+.
     *
     * @parameter property="optionalGettersForNullableFieldsOnly"
     */
    val optionalGettersForNullableFieldsOnly: Property<Boolean> =
        objects.property(Boolean::class.java).convention(false)


    /**
     * Determines whether or not to create setters for the fields of the record. The
     * default is to create setters.
     *
     * @parameter default-value="true"
     */
    val createSetters: Property<Boolean> = objects.property(Boolean::class.java).convention(true)

    /**
     * If set to true, @Nullable and @NotNull annotations are
     * added to fields of the record. The default is false. If enabled, JetBrains
     * annotations are used by default but other annotations can be specified via
     * the nullSafeAnnotationNullable and nullSafeAnnotationNotNull parameters.
     *
     * @parameter property="createNullSafeAnnotations"
     *
     * @see [
     * JetBrains nullability annotations](https://www.jetbrains.com/help/idea/annotating-source-code.html.nullability-annotations)
     */
    val createNullSafeAnnotations: Property<Boolean> = objects.property(Boolean::class.java).convention(false)

    /**
     * Controls which annotation should be added to nullable fields if
     * createNullSafeAnnotations is enabled. The default is
     * org.jetbrains.annotations.Nullable.
     *
     * @parameter property="nullSafeAnnotationNullable"
     *
     * @see [
     * JetBrains nullability annotations](https://www.jetbrains.com/help/idea/annotating-source-code.html.nullability-annotations)
     */
    val nullSafeAnnotationNullable: Property<String> =
        objects.property(String::class.java).convention("org.jetbrains.annotations.Nullable")

    /**
     * Controls which annotation should be added to non-nullable fields if
     * createNullSafeAnnotations is enabled. The default is
     * org.jetbrains.annotations.NotNull.
     *
     * @parameter property="nullSafeAnnotationNotNull"
     *
     * @see [
     * JetBrains nullability annotations](https://www.jetbrains.com/help/idea/annotating-source-code.html.nullability-annotations)
     */
    val nullSafeAnnotationNotNull: Property<String> =
        objects.property(String::class.java).convention("org.jetbrains.annotations.NotNull")

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
     * A set of fully qualified class names of custom
     * [org.apache.avro.LogicalTypes.LogicalTypeFactory] implementations to
     * add to the compiler. The classes must be on the classpath at compile time and
     * whenever the Java objects are serialized.
     *
     * @parameter property="customLogicalTypeFactories"
     */
    val customLogicalTypeFactories: ListProperty<String> =
        objects.listProperty(String::class.java).convention(emptyList())


    /**
     * Determines whether or not to use Java classes for decimal types
     *
     * @parameter default-value="false"
     */
    val enableDecimalLogicalType: Property<Boolean> = objects.property(Boolean::class.java).convention(false)

}
