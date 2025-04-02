# Overview

This is a [Gradle](http://www.gradle.org/) plugin to allow easily performing Java code generation for [Apache Avro](http://avro.apache.org/).  It supports JSON schema declaration files, JSON protocol declaration files, and Avro IDL files.

[![Build Status](https://github.com/davidmc24/gradle-avro-plugin/workflows/CI%20Build/badge.svg)](https://github.com/davidmc24/gradle-avro-plugin/actions)

# Compatibility

**NOTE**: Pre-1.0 versions used a different publishing process/namespace.  It is strongly recommended to upgrade to a newer version.  Further details can be found in the [change log](CHANGES.md).

* Currently tested against Java 8, 11, and 17-19
    * Though not supported yet, tests are also run against Java 20 to provide early notification of potential incompatibilities.
    * Java 19 support requires Gradle 7.6 or higher (as per Gradle's release notes)
    * Java 18 support requires Gradle 7.5 or higher (as per Gradle's release notes)
    * Java 17 support requires Gradle 7.3 or higher (as per Gradle's release notes)
    * Java 16 support requires Gradle 7.0 or higher (as per Gradle's release notes)
    * Java 15 support requires Gradle 6.7 or higher (as per Gradle's release notes)
    * Java 14 support requires Gradle 6.3 or higher (as per Gradle's release notes)
    * Java 13 support requires Gradle 6.0 or higher
    * Java 8-12 support requires Gradle 5.1 or higher (versions lower than 5.1 are no longer supported)
* Currently built against Gradle 7.6
    * Currently tested against Gradle 5.1-5.6.4 and 6.0-7.6
* Currently built against Avro 1.11.1
    * Currently tested against Avro 1.11.0-1.11.1
    * Avro 1.9.0-1.10.2 were last supported in version 1.2.1 
* Support for Kotlin
    * Dropped integration with the Kotlin plugin in plugin version 1.4.0, as Kotlin 1.7.x would require compile-time dependency on a specific Kotlin version
      * Wiring between the tasks added by the plugin and the Kotlin compilation tasks can either be added by your build logic, or a derived plugin
    * Plugin version 1.3.0 was the last version with tested support for Kotlin 
      * It is believed to work with Kotlin 1.6.x as well
      * It was tested against Kotlin plugin versions 1.3.20-1.3.72 and 1.4.0-1.4.32 and 1.5.0-1.5.31 using the latest compatible version of Gradle
      * It was tested against Kotlin plugin versions 1.2.20-1.2.71 and 1.3.0-1.3.11 using Gradle 5.1
    * Kotlin plugin versions 1.4.20-1.4.32 require special settings to work with Java 17+; see [KT-43704](https://youtrack.jetbrains.com/issue/KT-43704#focus=Comments-27-4639603.0-0)
    * Kotlin plugin version 1.3.30 is not compatible with Gradle 7.0+
    * Kotlin plugin versions starting with 1.4.0 require Gradle 5.3+
    * Kotlin plugin versions prior to 1.3.20 do not support Gradle 6.0+
    * Kotlin plugin versions prior to 1.2.30 do not support Java 10+
    * Version of the Kotlin plugin prior to 1.2.20 are unlikely to work
* Support for Gradle Kotlin DSL

# Usage

Add the following to your build files.  Substitute the desired version based on [CHANGES.md](https://github.com/davidmc24/gradle-avro-plugin/blob/master/CHANGES.md).

`settings.gradle`:
```groovy
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}
```

`build.gradle`:
```groovy
plugins {
    id "com.github.davidmc24.gradle.plugin.avro" version "VERSION"
}
```

Additionally, ensure that you have an implementation dependency on Avro, such as:

```groovy
repositories {
    mavenCentral()
}
dependencies {
    implementation "org.apache.avro:avro:1.11.0"
}
```

If you now run `gradle build`, Java classes will be compiled from Avro files in `src/main/avro`.
Actually, it will attempt to process an "avro" directory in every `SourceSet` (main, test, etc.)

# Configuration

There are a number of configuration options supported in the `avro` block.

| option                               | default                            | description                                                                                                       |
|--------------------------------------|------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| createSetters                        | `true`                             | `createSetters` passed to Avro compiler                                                                           |
| createOptionalGetters                | `false`                            | `createOptionalGetters` passed to Avro compiler                                                                   |
| gettersReturnOptional                | `false`                            | `gettersReturnOptional` passed to Avro compiler                                                                   |
| optionalGettersForNullableFieldsOnly | `false`                            | `optionalGettersForNullableFieldsOnly` passed to Avro compiler                                                    |
| fieldVisibility                      | `"PRIVATE"`                        | `fieldVisibility` passed to Avro compiler                                                                         |
| outputCharacterEncoding              | see below                          | `outputCharacterEncoding` passed to Avro compiler                                                                 |
| stringType                           | `"String"`                         | `stringType` passed to Avro compiler                                                                              |
| templateDirectory                    | see below                          | `templateDir` passed to Avro compiler                                                                             |
| additionalVelocityToolClasses        | see below                          | `additionalVelocityTools` passed to Avro compiler                                                                 |
| enableDecimalLogicalType             | `true`                             | `enableDecimalLogicalType` passed to Avro compiler                                                                |
| conversionsAndTypeFactoriesClasspath | empty `ConfigurableFileCollection` | used for loading custom conversions and logical type factories                                                    |
| logicalTypeFactoryClassNames         | empty `Map`                        | map from names to class names of logical types factories to be loaded from `conversionsAndTypeFactoriesClasspath` |
| customConversionClassNames           | empty `List`                       | class names of custom conversions to be loaded from `conversionsAndTypeFactoriesClasspath`                        |

Additionally, the `avro` extension exposes the following methods:

* `logicalTypeFactory(String typeName, Class typeFactoryClass)`: register an additional logical type factory
* `customConversion(Class conversionClass)`: register a custom conversion

## createSetters

Valid values: `true` (default), `false`; supports equivalent `String` values

Set to `false` to not create setter methods in the generated classes.

Example:

```groovy
avro {
    createSetters = false
}
```

## createOptionalGetters

Valid values: `false` (default), `true`; supports equivalent `String` values

Set to `true` to create additional getter methods that return their fields wrapped in an
[Optional](https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html). For a field with
name `abc` and type `string`, this setting will create a method
`Optional<java.lang.String> getOptionalAbc()`.

Example:

```groovy
avro {
    createOptionalGetters = false
}
```

## gettersReturnOptional

Valid values: `false` (default), `true`; supports equivalent `String` values

Set to `true` to cause getter methods to return
[Optional](https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html) wrappers of the
underlying type. Where [`createOptionalGetters`](#createoptionalgetters) generates an additional
method, this one replaces the existing getter.

Example:

```groovy
avro {
    gettersReturnOptional = false
}
```

## optionalGettersForNullableFieldsOnly

Valid values: `false` (default), `true`; supports equivalent `String` values

Set to `true` in conjuction with `gettersReturnOptional` to `true` to return
[Optional](https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html) wrappers of the
underlying type. Where [`gettersReturnOptional`](#gettersReturnOptional) alone changes all getters to
return `Optional`, this one only returns Optional for nullable (null union) field definitions.
Setting this to `true` without setting `gettersReturnOptional` to `true` will result in this flag having no effect.

Example:
```groovy
avro {
    gettersReturnOptional = true
    optionalGettersForNullableFieldsOnly = true
}
```

## fieldVisibility

Valid values: any [FieldVisibility](https://avro.apache.org/docs/1.11.0/api/java/org/apache/avro/compiler/specific/SpecificCompiler.FieldVisibility.html) or equivalent `String` name (matched case-insensitively); default `"PRIVATE"` (default)

By default, the fields in generated Java files will have private visibility.
Set to `"PRIVATE"` to explicitly specify private visibility of the fields, or `"PUBLIC"` to specify public visibility of the fields.

Example:

```groovy
avro {
    fieldVisibility = "PUBLIC"
}
```

## outputCharacterEncoding

Valid values: any [Charset](http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html) or equivalent `String` name

Controls the character encoding of generated Java files.
If using the plugin's conventions (i.e., not just the base plugin), the associated `JavaCompile` task's encoding will be used automatically.
Otherwise, it will use the value configured in the `avro` block, defaulting to `"UTF-8"`.

Examples:

```groovy
// Option 1: configure compilation task (avro plugin will automatically match)
tasks.withType(JavaCompile).configureEach {
    options.encoding = 'UTF-8'
}
// Option 2: just configure avro plugin
avro {
    outputCharacterEncoding = "UTF-8"
}
```

## stringType

Valid values: any [StringType](http://avro.apache.org/docs/1.8.1/api/java/org/apache/avro/generic/GenericData.StringType.html) or equivalent `String` name (matched case-insensitively); default `"String"` (default)

By default, the generated Java files will use [`java.lang.String`](http://docs.oracle.com/javase/7/docs/api/java/lang/String.html) to represent string types.
Alternatively, you can set it to `"Utf8"` to use [`org.apache.avro.util.Utf8`](https://avro.apache.org/docs/1.8.1/api/java/org/apache/avro/util/Utf8.html) or `"charSequence"` to use [`java.lang.CharSequence`](http://docs.oracle.com/javase/7/docs/api/java/lang/CharSequence.html).

```groovy
avro {
    stringType = "CharSequence"
}
```

## templateDirectory

By default, files will be generated using Avro's default templates.
If desired, you can override the template set used by either setting this property or the `"org.apache.avro.specific.templates"` System property.

```groovy
avro {
    templateDirectory = "/path/to/velocity/templates"
}
```

## additionalVelocityToolClasses

When overriding the default set of Velocity templates provided with Avro, it is often desirable to provide additional tools to use during generation. 
The class names you provide will be made available for use in your Velocity templates. An instance of each class provided will be created using 
the default constructor (required). When registered, they will be available as $class.simpleName(). Given the example configuration below,
two tools would be registered, and be available as escape and json.
 

```groovy
avro {
  additionalVelocityToolClasses = ['com.yourpackage.Escape', 'com.yourpackage.JSON']
}
```

## enableDecimalLogicalType

Valid values: `true` (default), `false`; supports equivalent `String` values

By default, generated Java files will use [`java.math.BigDecimal`](https://docs.oracle.com/javase/7/docs/api/java/math/BigDecimal.html)
for representing `fixed` or `bytes` fields annotated with `"logicalType": "decimal"`.
Set to `false` to use [`java.nio.ByteBuffer`](https://docs.oracle.com/javase/7/docs/api/java/nio/ByteBuffer.html) in generated classes.

Example:

```groovy
avro {
    enableDecimalLogicalType = false
}
```

## conversionsAndTypeFactoriesClasspath, logicalTypeFactoryClassNames and customConversionClassNames

Properties that can be used for loading [Conversion](https://avro.apache.org/docs/current/api/java/org/apache/avro/Conversion.html) and [LogicalTypeFactory](https://avro.apache.org/docs/current/api/java/org/apache/avro/LogicalTypes.LogicalTypeFactory.html) classes from outside of the build classpath.

Example:

```groovy
configurations {
    customConversions
}

dependencies {
    customConversions(project(":custom-conversions"))
}

avro {
    conversionsAndTypeFactoriesClasspath.from(configurations.customConversions)
    logicalTypeFactoryClassNames.put("timezone", "com.github.davidmc24.gradle.plugin.avro.test.custom.TimeZoneLogicalTypeFactory")
    customConversionClassNames.add("com.github.davidmc24.gradle.plugin.avro.test.custom.TimeZoneConversion")
}
```

# IntelliJ Integration

The plugin attempts to make IntelliJ play more smoothly with generated sources when using Gradle-generated project files.
However, there are still some rough edges.  It will work best if you first run `gradle build`, and _after_ that run `gradle idea`.
If you do it in the other order, IntelliJ may not properly exclude some directories within your `build` directory.

# Alternate Usage

If the defaults used by the plugin don't work for you, you can still use the tasks by themselves.
In this case, use the `com.github.davidmc24.gradle.plugin.avro-base` plugin instead, and create tasks of type `GenerateAvroJavaTask` and/or `GenerateAvroProtocolTask`.

Here's a short example of what this might look like:

```groovy
import com.github.davidmc24.gradle.plugin.avro.GenerateAvroJavaTask

apply plugin: "java"
apply plugin: "com.github.davidmc24.gradle.plugin.avro-base"

dependencies {
    implementation "org.apache.avro:avro:1.11.0"
}

def generateAvro = tasks.register("generateAvro", GenerateAvroJavaTask) {
    source("src/avro")
    outputDir = file("dest/avro")
}

tasks.named("compileJava").configure {
    source(generateAvro)
}
```

# File Processing

When using this plugin, it is recommended to define each record/enum/fixed type in its own file rather than using inline type definitions.
This approach allows defining any type of schema structure, and eliminates the potential for conflicting definitions of a type between multiple files.
The plugin will automatically recognize the dependency and compile the files in the correct order.
For example, instead of `Cat.avsc`:

```json
{
    "name": "Cat",
    "namespace": "example",
    "type": "record",
    "fields" : [
        {
            "name": "breed",
            "type": {
                "name": "Breed",
                "type": "enum",
                "symbols" : [
                    "ABYSSINIAN", "AMERICAN_SHORTHAIR", "BIRMAN", "MAINE_COON", "ORIENTAL", "PERSIAN", "RAGDOLL", "SIAMESE", "SPHYNX"
                ]
            }
        }
    ]
}
```

use `Breed.avsc`:

```json
{
    "name": "Breed",
    "namespace": "example",
    "type": "enum",
    "symbols" : ["ABYSSINIAN", "AMERICAN_SHORTHAIR", "BIRMAN", "MAINE_COON", "ORIENTAL", "PERSIAN", "RAGDOLL", "SIAMESE", "SPHYNX"]
}
```


and `Cat.avsc`:

```json
{
    "name": "Cat",
    "namespace": "example",
    "type": "record",
    "fields" : [
        {"name": "breed", "type": "Breed"}
    ]
}
```

There may be cases where the schema files contain inline type definitions and it is undesirable to modify them.
In this case, the plugin will automatically recognize any duplicate type definitions and check if they match.
If any conflicts are identified, it will cause a build failure.

# Kotlin Support

The Java classes generated from your Avro files should be automatically accessible in the classpath to Kotlin classes in the same sourceset, and transitively to any sourcesets that depend on that sourceset.
This is accomplished by this plugin detecting that the Kotlin plugin has been applied, and informing the Kotlin compilation tasks of the presence of the generated sources directories for cross-compilation.

This support does *not* support producing the Avro generated classes as Kotlin classes, as that functionality is not currently provided by the upstream Avro library.

# Kotlin DSL Support

Special notes relevant to using this plugin via the Gradle Kotlin DSL:

* Apply the plugin declaratively using the `plugins {}` block.  Otherwise, various features may not work as intended.  See [Configuring Plugins in the Gradle Kotlin DSL](https://github.com/gradle/kotlin-dsl/blob/master/doc/getting-started/Configuring-Plugins.md) for more details.
* Configuration in the `avro {}` block must be applied differently than in the Groovy DSL.  See the example below for details.

### Example Kotlin DSL Setup:

In `gradle.build.kts` add:

```kotlin
plugins {
    // Find latest release here: https://github.com/davidmc24/gradle-avro-plugin/releases
    id("com.github.davidmc24.gradle.plugin.avro") version "VERSION"
}
```

And then in your `settings.gradle.kts` add:

```kotlin
pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}
```

The syntax for configuring the extension looks like this:

```kotlin
avro {
    isCreateSetters.set(true)
    isCreateOptionalGetters.set(false)
    isGettersReturnOptional.set(false)
    isOptionalGettersForNullableFieldsOnly.set(false)
    fieldVisibility.set("PUBLIC_DEPRECATED")
    outputCharacterEncoding.set("UTF-8")
    stringType.set("String")
    templateDirectory.set(null as String?)
    isEnableDecimalLogicalType.set(true)
}
```

# Resolving schema dependencies

If desired, you can generate JSON schema with dependencies resolved.

Example build:

```groovy
import com.github.davidmc24.gradle.plugin.avro.ResolveAvroDependenciesTask

apply plugin: "com.github.davidmc24.gradle.plugin.avro-base"

tasks.register("resolveAvroDependencies", ResolveAvroDependenciesTask) {
    source file("src/avro/normalized")
    outputDir = file("build/avro/resolved")
}
```

# Generating schema files from protocol/IDL

If desired, you can generate JSON schema files.
To do this, apply the plugin (either `avro` or `avro-base`), and define custom tasks as needed for the schema generation.
From JSON protocol files, all that's needed is the `GenerateAvroSchemaTask`.
From IDL files, first use `GenerateAvroProtocolTask` to convert the IDL files to JSON protocol files, then use `GenerateAvroSchemaTask`.

Example using base plugin with support for both IDL and JSON protocol files in `src/main/avro`:

```groovy
import com.github.davidmc24.gradle.plugin.avro.GenerateAvroProtocolTask
import com.github.davidmc24.gradle.plugin.avro.GenerateAvroSchemaTask

apply plugin: "com.github.davidmc24.gradle.plugin.avro-base"

def generateProtocol = tasks.register("generateProtocol", GenerateAvroProtocolTask) {
    source file("src/main/avro")
    include("**/*.avdl")
    outputDir = file("build/generated-avro-main-avpr")
}

tasks.register("generateSchema", GenerateAvroSchemaTask) {
    dependsOn generateProtocol
    source file("src/main/avro")
    source file("build/generated-avro-main-avpr")
    include("**/*.avpr")
    outputDir = file("build/generated-main-avro-avsc")
}
```
