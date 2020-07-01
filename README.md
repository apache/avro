# Overview

This is a [Gradle](http://www.gradle.org/) plugin to allow easily performing Java code generation for [Apache Avro](http://avro.apache.org/).  It supports JSON schema declaration files, JSON protocol declaration files, and Avro IDL files.

[![Build Status](https://github.com/davidmc24/gradle-avro-plugin/workflows/CI%20Build/badge.svg)](https://github.com/davidmc24/gradle-avro-plugin/actions)

# Compatibility

* Currently tested against Java 8-14
    * Java 15 support appears to require Gradle 6.3 or higher (currently tested against pre-release versions)
    * Java 14 support requires Gradle 6.3 or higher
    * Java 13 support requires Gradle 6.0 or higher
    * Java 11 support requires Gradle 4.8 or higher
    * Though not supported yet, tests are also run against early-access builds of Java 15 to provide early notification of potential incompatibilities
    * If you need support for Java 7, version 0.16.0 was the last supported version
    * If you need support for Java 6, version 0.9.1 was the last supported version; please see [the Gradle plugin portal](https://plugins.gradle.org/plugin/com.commercehub.gradle.plugin.avro)
* Currently built against Gradle 6.5
    * Currently tested against Gradle 5.1-5.6.4 and 6.0-6.5
    * If you need support for Gradle 4.4-5.0, version 0.18.0 was the last version tested for compatibility
    * If you need support for Gradle 3.0-3.5.1 or 4.0-4.3, version 0.17.0 was the last version tested for compatibility
    * If you need support for Gradle 2.0-2.14.1, version 0.9.1 was the last version tested for compatibility; please see [the Gradle plugin portal](https://plugins.gradle.org/plugin/com.commercehub.gradle.plugin.avro)
* Currently built against Avro 1.10.0
    * Currently tested against Avro 1.10.0
    * If you need support for Avro 1.9.0-1.9.2 try plugin version 0.20.0
    * If you need support for Avro 1.8.2, try plugin version 0.16.0
    * If you need support for Avro 1.8.0-1.8.1, try plugin version 0.10.0
    * If you need support for Avro 1.7.7, try plugin version 0.8.1 (updated for Gradle 5.6)
    * Versions of Avro prior to 1.7.x are unlikely to work
* Support for Kotlin
    * Currently tested against Kotlin plugin versions 1.3.20-1.3.61 using the latest supported version of Gradle
    * Currently tested against Kotlin plugin versions 1.2.20-1.2.71 and 1.3.0-1.3.11 using Gradle 5.1
    * Kotlin plugin versions prior to 1.2.30 do not support Java 10+
    * Version of the Kotlin plugin prior to 1.2.20 are unlikely to work
* Support for Gradle Kotlin DSL

# Usage

Add the following to your `build.gradle` file.  Substitute the desired version based on [CHANGES.md](https://github.com/davidmc24/gradle-avro-plugin/blob/master/CHANGES.md).

```groovy
buildscript {
    repositories {
        jcenter()
    }
    dependencies {
        classpath "com.commercehub.gradle.plugin:gradle-avro-plugin:VERSION"
    }
}
apply plugin: "com.commercehub.gradle.plugin.avro"
```

Additionally, ensure that you have a compile dependency on Avro, such as:

```groovy
repositories {
    jcenter()
}
dependencies {
    compile "org.apache.avro:avro:1.10.0"
}
```

If you now run `gradle build`, Java classes will be compiled from Avro files in `src/main/avro`.
Actually, it will attempt to process an "avro" directory in every `SourceSet` (main, test, etc.)

Alternatively, if you prefer to use the incubating plugins DSL, see the following example:

`settings.gradle`:
```groovy
pluginManagement {
    repositories {
        gradlePluginPortal()
        jcenter()
        maven {
            name "JCenter Gradle Plugins"
            url  "https://dl.bintray.com/gradle/gradle-plugins"
        }
    }
}
```

`build.gradle`:
```groovy
plugins {
    id "com.commercehub.gradle.plugin.avro" version "VERSION"
}
```

# Configuration

There are a number of configuration options supported in the `avro` block.

| option                               | default               | description                                                    |
| -------------------------------------| --------------------- | ---------------------------------------------------------------|
| createSetters                        | `true`                | `createSetters` passed to Avro compiler                        |
| createOptionalGetters                | `false`               | `createOptionalGetters` passed to Avro compiler                |
| gettersReturnOptional                | `false`               | `gettersReturnOptional` passed to Avro compiler                |
| optionalGettersForNullableFieldsOnly | `false`               | `optionalGettersForNullableFieldsOnly` passed to Avro compiler |
| fieldVisibility                      | `"PUBLIC_DEPRECATED"` | `fieldVisibility` passed to Avro compiler                      |
| outputCharacterEncoding              | see below             | `outputCharacterEncoding` passed to Avro compiler              |
| stringType                           | `"String"`            | `stringType` passed to Avro compiler                           |
| templateDirectory                    | see below             | `templateDir` passed to Avro compiler                          |
| enableDecimalLogicalType             | `true`                | `enableDecimalLogicalType` passed to Avro compiler             |

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

Valid values: any [FieldVisibility](http://avro.apache.org/docs/1.8.1/api/java/org/apache/avro/compiler/specific/SpecificCompiler.FieldVisibility.html) or equivalent `String` name (matched case-insensitively); default `"PUBLIC_DEPRECATED"` (default)

By default, the fields in generated Java files will have public visibility and be annotated with `@Deprecated`.
Set to `"PRIVATE"` to restrict visibility of the fields, or `"PUBLIC"` to remove the `@Deprecated` annotations.

Example:

```groovy
avro {
    fieldVisibility = "PRIVATE"
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

# IntelliJ Integration

The plugin attempts to make IntelliJ play more smoothly with generated sources when using Gradle-generated project files.
However, there are still some rough edges.  It will work best if you first run `gradle build`, and _after_ that run `gradle idea`.
If you do it in the other order, IntelliJ may not properly exclude some directories within your `build` directory.

# Alternate Usage

If the defaults used by the plugin don't work for you, you can still use the tasks by themselves.
In this case, use the `com.commercehub.gradle.plugin.avro-base` plugin instead, and create tasks of type `GenerateAvroJavaTask` and/or `GenerateAvroProtocolTask`.

Here's a short example of what this might look like:

```groovy
apply plugin: "java"
apply plugin: "com.commercehub.gradle.plugin.avro-base"

dependencies {
    implementation "org.apache.avro:avro:1.10.0"
}

def generateAvro = tasks.register("generateAvro", com.commercehub.gradle.plugin.avro.GenerateAvroJavaTask) {
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
	id("com.commercehub.gradle.plugin.avro") version "VERSION"
}
```

And then in your `settings.gradle.kts` add:

```kotlin
pluginManagement {
	repositories {
		gradlePluginPortal()
		jcenter()
		maven (url="https://dl.bintray.com/gradle/gradle-plugins")
	}
}
```

The syntax for configuring the extension looks like this:

```kotlin
avro {
    isCreateSetters.set(true)
    isCreateOptionalGetters.set(false)
    isGettersReturnOptional.set(false)
    isOptionalGettersForNullableFieldsOnly(false)
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
apply plugin: "com.commercehub.gradle.plugin.avro-base"

tasks.register("resolveAvroDependencies", com.commercehub.gradle.plugin.avro.ResolveAvroDependenciesTask) {
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
apply plugin: "com.commercehub.gradle.plugin.avro-base"

def generateProtocol = tasks.register("generateProtocol", com.commercehub.gradle.plugin.avro.GenerateAvroProtocolTask) {
    source file("src/main/avro")
    include("**/*.avdl")
    outputDir = file("build/generated-avro-main-avpr")
}

tasks.register("generateSchema", com.commercehub.gradle.plugin.avro.GenerateAvroSchemaTask) {
    dependsOn generateProtocol
    source file("src/main/avro")
    source file("build/generated-avro-main-avpr")
    include("**/*.avpr")
    outputDir = file("build/generated-main-avro-avsc")
}
```
