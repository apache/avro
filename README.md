# Overview

This is a [Gradle](http://www.gradle.org/) plugin to allow easily performing Java code generation for [Apache Avro](http://avro.apache.org/).  It supports JSON schema declaration files, JSON protocol declaration files, and Avro IDL files.

[![Build Status](https://travis-ci.org/commercehub-oss/gradle-avro-plugin.svg?branch=master)](https://travis-ci.org/commercehub-oss/gradle-avro-plugin)

# Compatibility

* Currently tested against Java 7-11
    * Java 11 support requires Gradle 4.8 or higher
    * Java 9 support requires Gradle 4.2.1 or higher
    * If you need support for Java 6, version 0.9.1 was the last supported version
* Currently built against Gradle 4.8
    * Currently tested against Gradle 3.0-3.5.1 and 4.0-4.8
    * If you need support for Gradle 2.0-2.14.1, version 0.9.1 was the last version tested for compatibility
    * Other versions may be compatible, but Gradle 1.x versions are unlikely to work
* Currently built against Avro 1.8.2
    * Currently tested against Avro 1.8.2; other versions may be compatible
    * If you need support for Avro 1.8.0-1.8.1, try plugin version 0.10.0
    * If you need support for Avro 1.7.x, try plugin version 0.8.0; versions of Avro before that are unlikely to work
* Incubating: support for Kotlin
    * Currently tested against Kotlin 1.2.31
    * Kotlin 1.1.2 and higher requires Java 8+
    * Doesn't work with Gradle 3.2-3.2.1

# Usage

Add the following to your `build.gradle` file.  Substitute the desired version based on [CHANGES.md](https://github.com/commercehub-oss/gradle-avro-plugin/blob/master/CHANGES.md).

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
    compile "org.apache.avro:avro:1.8.2"
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

| option                    | default               | description                                           |
| --------------------------| --------------------- | -------------------------------------------------     |
| createSetters             | `true`                | `createSetters` passed to Avro compiler               |
| fieldVisibility           | `"PUBLIC_DEPRECATED"` | `fieldVisibility` passed to Avro compiler             |
| outputCharacterEncoding   | see below             | `outputCharacterEncoding` passed to Avro compiler     |
| stringType                | `"String"`            | `stringType` passed to Avro compiler                  |
| templateDirectory         | see below             | `templateDir` passed to Avro compiler                 |
| enableDecimalLogicalType  | `true`                | `enableDecimalLogicalType` passed to Avro compiler    |
| validateDefaults          | `false`               | `setValidateDefaults` set on Avro Schema Parser       |

## createSetters

Valid values: `true` (default), `false`; supports equivalent `String` values

Set to `false` to not create setter methods in the generated classes.

Example:

```groovy
avro {
    createSetters = false
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
If the associated `JavaCompile` task has a configured encoding, it will be used automatically.
Otherwise, it will use the value configured in the `avro` block, defaulting to `"UTF-8"`.

Examples:

```groovy
// Option 1: configure compilation task (avro plugin will automatically match)
tasks.withType(JavaCompile) {
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

## validateDefaults

Valid values: `false` (default), `true`; supports equivalent `String` values

Set to `true` to force validation of default values in schema files. Each validation error will cause the build to fail.
In order to maintain backward compatibility this option is disabled by default, but in the next big release, this option will
be removed and schema validation will always be enabled.

Example:

```groovy
avro {
    validateDefaults = true
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
    compile "org.apache.avro:avro:1.8.2"
}

task generateAvro(type: com.commercehub.gradle.plugin.avro.GenerateAvroJavaTask) {
    source("src/avro")
    outputDir = file("dest/avro")
}

compileJava.source(generateAvro.outputs)
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
