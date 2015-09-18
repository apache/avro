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

Optionally, configure the string type to `charSequence`, `string` (the default), or `utf8`.

```groovy
avro {
    stringType = "string"
}
```

Optionally, you can also configure the output character encoding (default `UTF-8`).

```groovy
avro {
    encoding = "UTF-8"
}
```

Optionally, you can also configure the visibility of generated fields to `PRIVATE`, `PUBLIC_DEPRECATED` (the default)
or `PUBLIC`.

```groovy
avro {
    fieldVisibility = "PRIVATE"
}
```

Optionally, you can also configure the source directory for the Velocity templates that the Avro compiler uses to
generate Java files.

```groovy
avro {
    templateDirectory = "/path/to/velocity/templates"
}
```

Additionally, ensure that you have a compile dependency on avro, such as:

```groovy
repositories {
    jcenter()
}
dependencies {
    compile "org.apache.avro:avro:1.7.7"
}
```

If you now run `gradle build`, Java classes will be compiled from Avro files in `src/main/avro`.  Actually, it will attempt to process an "avro" directory in every `SourceSet` (main, test, etc.)

# IntelliJ Integration

The plugin attempts to make IntelliJ play more smoothly with generated sources when using Gradle-generated project files.  However, there are still some rough edges.  It will work best if you first run `gradle build`, and _after_ that run `gradle idea`.  If you do it in the other order, IntelliJ may not properly exclude some directories within your `build` directory.

# Alternate Usage

If the defaults used by the plugin don't work for you, you can still use the tasks by themselves.  In this case, use the "com.commercehub.gradle.plugin.avro" plugin instead, and create tasks of type `GenerateAvroJavaTask` and/or `GenerateAvroProtocolTask`.

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

# Handling dependencies

If you have record/enum/fixed types that are referenced in other record types, just define the shared type in a separate file rather than inline in the definition of each record type that uses it.  The plugin will automatically recognize the dependency and compile the files in the correct order.  For example, instead of `Cat.avsc`:

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
