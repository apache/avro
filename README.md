# Overview

This is a [Gradle](http://www.gradle.org/) plugin to allow easily performing Java code generation for [Apache Avro](http://avro.apache.org/).  It supports JSON schema declaration files, JSON protocol declaration files, and Avro IDL files.

[![Build Status](https://travis-ci.org/commercehub-oss/gradle-avro-plugin.svg?branch=master)](https://travis-ci.org/commercehub-oss/gradle-avro-plugin)

# Compatibility

* Currently tested against Gradle 2.7; other versions may be compatible
* Currently tested against Avro 1.7.7; other versions may be compatible
* Java 7 or higher required

# Usage

Add the following to your `build.gradle` file.  Substitute the desired version based on [CHANGES.md](https://github.com/commercehub-oss/gradle-avro-plugin/blob/master/CHANGES.md).

```groovy
// Gradle 2.1 and later
plugins {
    id "com.commercehub.gradle.plugin.avro" version "VERSION"
}

// Earlier versions of Gradle
buildscript {
    repositories {
        maven {
            url "https://plugins.gradle.org/m2/"
        }
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
    compile "org.apache.avro:avro:1.7.7"
}
```

If you now run `gradle build`, Java classes will be compiled from Avro files in `src/main/avro`.
Actually, it will attempt to process an "avro" directory in every `SourceSet` (main, test, etc.)

# Configuration

There are a number of configuration options supported in the `avro` block.

| option            | default               | description                                       |
| ----------------- | --------------------- | ------------------------------------------------- |
| createSetters     | `true`                | `createSetters` passed to Avro compiler           |
| encoding          | `"UTF-8"`             | `outputCharacterEncoding` passed to Avro compiler |
| fieldVisibility   | `"PUBLIC_DEPRECATED"` | `fieldVisibility` passed to Avro compiler         |
| stringType        | `"String"`            | `stringType` passed to Avro compiler              |
| templateDirectory | see below             | `templateDir` passed to Avro compiler             |

## createSetters

Valid values: `true` (default), `false`

Set to `false` to not create setter methods in the generated classes.

Example:

```groovy
avro {
    createSetters = false
}
```

## encoding

Valid values: any charset name supported by [Charset](http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html), default `"UTF-8"`

By default, the plugin will output generated Java files in UTF-8.
If desired, you can specify an alternate encoding.

Example:

```groovy
avro {
    encoding = "UTF-8"
}
```

## fieldVisibility

Valid values (matched case-insensitively): `"PUBLIC"`, `"PUBLIC_DEPRECATED"` (default), `"PRIVATE"`

By default, the fields in generated Java files will have public visibility and be annotated with `@Deprecated`.
Set to `"PRIVATE"` to restrict visibility of the fields, or `"PUBLIC"` to remove the `@Deprecated` annotations.

Example:

```groovy
avro {
    fieldVisibility = "PRIVATE"
}
```

## stringType

Valid values (matched case-insensitively): `"CharSequence"`, `"String"` (default), `"Utf8"`

By default, the generated Java files will use [`java.lang.String`](http://docs.oracle.com/javase/7/docs/api/java/lang/String.html) to represent string types.
Alternatively, you can set it to `"Utf8"` to use [`org.apache.avro.util.Utf8`](https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/util/Utf8.html) or `"charSequence"` to use [`java.lang.CharSequence`](http://docs.oracle.com/javase/7/docs/api/java/lang/CharSequence.html).

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

# IntelliJ Integration

The plugin attempts to make IntelliJ play more smoothly with generated sources when using Gradle-generated project files.
However, there are still some rough edges.  It will work best if you first run `gradle build`, and _after_ that run `gradle idea`.
If you do it in the other order, IntelliJ may not properly exclude some directories within your `build` directory.

# Alternate Usage

If the defaults used by the plugin don't work for you, you can still use the tasks by themselves.
In this case, use the "com.commercehub.gradle.plugin.avro" plugin instead, and create tasks of type `GenerateAvroJavaTask` and/or `GenerateAvroProtocolTask`.

Here's a short example of what this might look like:

```groovy
apply plugin: "java"
apply plugin: "com.commercehub.gradle.plugin.avro-base"

dependencies {
    compile "org.apache.avro:avro:1.7.7"
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
